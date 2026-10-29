"""MeldRx raw-layer ingest (reference implementation for the test setup).

Copies every new source file version into the raw layer, splits it into one JSON line per FHIR
resource, keeps each resource whole as JSON text, registers Glue tables/partitions (no crawler)
and writes the untouched landing copy LAST as the "done" mark.

Environment variables
  MELDRX_SOURCE_URI      s3://<source-bucket>/<prefix>/      e.g. s3://src/patient_access/
  MELDRX_RAW_URI         s3://<raw-bucket>/<prefix>/         e.g. s3://raw/meldrx/
  MELDRX_RAW_DB          Glue database for meldrx_raw_<resource> tables
  MELDRX_KMS_KEY         KMS key id / ARN / alias used to encrypt everything we write
  MELDRX_INGEST_ENABLED  "false" skips ingest (kill switch). Default "true"
  MELDRX_REBUILD         "true" rebuilds resources/ from landing/ instead of reading the source
"""
import datetime as dt
import hashlib
import json
import os
import re
import sys
import time

import boto3
from botocore.exceptions import ClientError

COLUMNS = ["participant_id", "resource_type", "resource_id", "created_at", "last_updated", "full_url",
           "response_json", "package_metadata_json", "resource_json",
           "source_file", "source_version_id", "content_sha256"]
SERDE = "org.openx.data.jsonserde.JsonSerDe"
INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat"
OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"


def log(event, **kw):
    print(json.dumps({"ts": dt.datetime.now(dt.timezone.utc).isoformat(), "event": event, **kw}), flush=True)


def split_uri(uri):
    m = re.match(r"s3://([^/]+)/?(.*)$", uri)
    if not m:
        raise SystemExit(f"bad S3 URI: {uri}")
    prefix = m.group(2)
    return m.group(1), (prefix if not prefix or prefix.endswith("/") else prefix + "/")


class Ingest:
    def __init__(self, s3=None, glue=None):
        self.src_bucket, self.src_prefix = split_uri(os.environ["MELDRX_SOURCE_URI"])
        self.raw_bucket, self.raw_prefix = split_uri(os.environ["MELDRX_RAW_URI"])
        self.db = os.environ["MELDRX_RAW_DB"]
        self.kms = os.environ["MELDRX_KMS_KEY"]
        self.s3 = s3 or boto3.client("s3")
        self.glue = glue or boto3.client("glue")
        self.tables, self.partitions = set(), set()
        self.stats = {"new_versions": 0, "files_loaded": 0, "entries": {}, "quarantined": 0,
                      "deletions": 0, "resource_files_deleted": 0, "failed": 0, "per_file": []}

    # ---------- S3 helpers
    def _keys(self, bucket, prefix):
        out = set()
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            out.update(o["Key"] for o in page.get("Contents", []))
        return out

    def _put(self, key, body, metadata=None):
        self.s3.put_object(Bucket=self.raw_bucket, Key=key, Body=body, ServerSideEncryption="aws:kms",
                           SSEKMSKeyId=self.kms, ContentType="application/json", Metadata=metadata or {})

    def _delete_resource_files_for_source_versions(self, participant, fname, source_key, deleted_at):
        """Delete derived resource objects for source versions that existed at deletion time.

        Resource object names end with {participant}__{fname}__{source_version_id}.json.
        Restrict deletion to versions whose LastModified is at or before this delete marker,
        so a later re-upload of the same source filename is not removed by an older marker.
        Landing copies are deliberately retained as the immutable audit/source history.
        """
        source_version_ids = set()
        for page in self.s3.get_paginator("list_object_versions").paginate(
                Bucket=self.src_bucket, Prefix=source_key):
            for version in page.get("Versions", []):
                if (version["Key"] == source_key
                        and version["LastModified"] <= deleted_at
                        and version["VersionId"] != "null"):
                    source_version_ids.add(version["VersionId"])

        if not source_version_ids:
            return 0

        resource_prefix = self.raw_prefix + "resources/"
        deleted_count = 0
        for key in self._keys(self.raw_bucket, resource_prefix):
            basename = key.rsplit("/", 1)[-1]
            for version_id in source_version_ids:
                expected_name = f"{participant}__{fname}__{version_id}.json"
                if basename == expected_name:
                    self.s3.delete_object(Bucket=self.raw_bucket, Key=key)
                    deleted_count += 1
                    break
        self.stats["resource_files_deleted"] += deleted_count
        return deleted_count

    # ---------- Glue helpers (no crawler)
    def _storage(self, location):
        return {"Columns": [{"Name": c, "Type": "string"} for c in COLUMNS], "Location": location,
                "InputFormat": INPUT_FORMAT, "OutputFormat": OUTPUT_FORMAT,
                "SerdeInfo": {"SerializationLibrary": SERDE, "Parameters": {"ignore.malformed.json": "false"}}}

    def ensure_table(self, rtype):
        name = "meldrx_raw_" + re.sub(r"[^a-z0-9]", "", rtype.lower())
        if name in self.tables:
            return name
        loc = f"s3://{self.raw_bucket}/{self.raw_prefix}resources/{rtype}/"
        try:
            self.glue.get_table(DatabaseName=self.db, Name=name)
        except self.glue.exceptions.EntityNotFoundException:
            self.glue.create_table(DatabaseName=self.db, TableInput={
                "Name": name, "TableType": "EXTERNAL_TABLE",
                "Parameters": {"classification": "json", "EXTERNAL": "TRUE"},
                "PartitionKeys": [{"Name": "delivery_date", "Type": "string"}],
                "StorageDescriptor": self._storage(loc)})
            log("table_created", table=name)
        self.tables.add(name)
        return name

    def ensure_partition(self, table, rtype, day):
        if (table, day) in self.partitions:
            return
        loc = f"s3://{self.raw_bucket}/{self.raw_prefix}resources/{rtype}/delivery_date={day}/"
        try:
            self.glue.create_partition(DatabaseName=self.db, TableName=table,
                                       PartitionInput={"Values": [day], "StorageDescriptor": self._storage(loc)})
        except self.glue.exceptions.AlreadyExistsException:
            pass
        self.partitions.add((table, day))

    # ---------- core
    @staticmethod
    def parse(body):
        doc = json.loads(body)
        meta, bundle = doc.get("package_metadata"), doc.get("bundle")
        if not isinstance(meta, dict) or "createdAt" not in meta:
            raise ValueError("package_metadata.createdAt missing")
        if not isinstance(bundle, dict) or not isinstance(bundle.get("entry"), list):
            raise ValueError("bundle.entry missing")
        for e in bundle["entry"]:
            if not isinstance(e, dict) or not isinstance(e.get("resource"), dict) or not e["resource"].get("resourceType"):
                raise ValueError("entry without resource.resourceType")
        return meta, bundle["entry"]

    def split(self, participant, fname, file_id, day, source_file, version_id, body, sha):
        meta, entries = self.parse(body)
        by_type = {}
        for e in entries:
            r = e["resource"]
            row = {"participant_id": participant, "resource_type": r["resourceType"], "resource_id": r.get("id"),
                   "created_at": meta.get("createdAt"), "last_updated": (r.get("meta") or {}).get("lastUpdated"),
                   "full_url": e.get("fullUrl"),
                   "response_json": json.dumps(e.get("response"), separators=(",", ":")) if "response" in e else None,
                   "package_metadata_json": json.dumps(meta, separators=(",", ":")),
                   "resource_json": json.dumps(r, separators=(",", ":"), ensure_ascii=False),
                   "source_file": source_file, "source_version_id": version_id, "content_sha256": sha}
            by_type.setdefault(r["resourceType"], []).append(json.dumps(row, ensure_ascii=False))
        for rtype, lines in by_type.items():
            key = f"{self.raw_prefix}resources/{rtype}/delivery_date={day}/{participant}__{fname}__{file_id}.json"
            self._put(key, ("\n".join(lines) + "\n").encode("utf-8"))
            table = self.ensure_table(rtype)
            self.ensure_partition(table, rtype, day)
            self.stats["entries"][rtype] = self.stats["entries"].get(rtype, 0) + len(lines)
        self.stats["per_file"].append({"source_file": source_file, "source_version_id": version_id,
                                       "file_id": file_id, "entries": len(entries),
                                       "by_type": {k: len(v) for k, v in by_type.items()}})

    def run(self):
        landing = self._keys(self.raw_bucket, self.raw_prefix + "landing/")
        quarantine = self._keys(self.raw_bucket, self.raw_prefix + "quarantine/")
        deletions = self._keys(self.raw_bucket, self.raw_prefix + "deletions/")
        pager = self.s3.get_paginator("list_object_versions")
        for page in pager.paginate(Bucket=self.src_bucket, Prefix=self.src_prefix):
            for v in page.get("Versions", []):
                if v["Key"].endswith(".json"):
                    try:
                        self.version(v, landing, quarantine)
                    except Exception as exc:          # keep going; the run still fails at the end
                        self.stats["failed"] += 1
                        log("file_failed", key=v["Key"], version_id=v["VersionId"], error=repr(exc))
            for m in page.get("DeleteMarkers", []):
                self.delete_marker(m, deletions)

    def names(self, key):
        rel = key[len(self.src_prefix):]
        participant, _, rest = rel.partition("/")
        return participant, rest.rsplit("/", 1)[-1][:-len(".json")]

    def version(self, v, landing, quarantine):
        participant, fname = self.names(v["Key"])
        vid = v["VersionId"]
        body, sha = None, None
        if vid == "null":                         # versioning off (non-prod): identity = content hash
            body = self.s3.get_object(Bucket=self.src_bucket, Key=v["Key"])["Body"].read()
            sha = hashlib.sha256(body).hexdigest()
            file_id = sha[:12]
        else:
            file_id = vid
        name = f"{participant}/{fname}__{file_id}.json"
        if self.raw_prefix + "landing/" + name in landing or self.raw_prefix + "quarantine/" + name in quarantine:
            return
        self.stats["new_versions"] += 1
        if body is None:
            body = self.s3.get_object(Bucket=self.src_bucket, Key=v["Key"], VersionId=vid)["Body"].read()
            sha = hashlib.sha256(body).hexdigest()
        day = v["LastModified"].astimezone(dt.timezone.utc).strftime("%Y-%m-%d")
        meta = {"source-file": v["Key"], "source-version-id": vid, "content-sha256": sha, "delivery-date": day}
        try:
            self.split(participant, fname, file_id, day, v["Key"], vid, body, sha)
        except (ValueError, UnicodeDecodeError) as exc:
            self._put(self.raw_prefix + "quarantine/" + name, body, {**meta, "reason": str(exc)[:200]})
            self.stats["quarantined"] += 1
            log("file_quarantined", key=v["Key"], version_id=vid, reason=str(exc))
            return
        self._put(self.raw_prefix + "landing/" + name, body, meta)     # LAST: marks the version done
        self.stats["files_loaded"] += 1
        log("file_loaded", key=v["Key"], version_id=vid, sha256=sha, delivery_date=day)

    def delete_marker(self, m, deletions):
        if not m["Key"].endswith(".json") or m["VersionId"] == "null":
            return
        participant, fname = self.names(m["Key"])
        key = f"{self.raw_prefix}deletions/{participant}/{fname}__{m['VersionId']}.json"

        # Delete resource-specific copies for source versions that existed before this
        # delete marker. This also repairs markers recorded by older runs that did not
        # remove the resource-specific objects. Newer re-uploads are left untouched.
        deleted_count = self._delete_resource_files_for_source_versions(
            participant, fname, m["Key"], m["LastModified"])
        log("resource_files_deleted_for_source_deletion", source_file=m["Key"],
            delete_marker_version_id=m["VersionId"], resource_files_deleted=deleted_count)

        # Keep the existing deletion audit record behavior; do not create duplicates.
        if key in deletions:
            return
        self._put(key, json.dumps({"source_file": m["Key"], "delete_marker_version_id": m["VersionId"],
                                   "deleted_at": m["LastModified"].isoformat(),
                                   "resource_files_deleted": deleted_count}).encode("utf-8"))
        self.stats["deletions"] += 1
        log("deletion_recorded", key=m["Key"], delete_marker_version_id=m["VersionId"])

    def rebuild(self):
        """Rewrite resources/ from the untouched landing copies (e.g. after a split-logic fix)."""
        for key in sorted(self._keys(self.raw_bucket, self.raw_prefix + "landing/")):
            obj = self.s3.get_object(Bucket=self.raw_bucket, Key=key)
            md, body = obj["Metadata"], obj["Body"].read()
            participant, fname_id = key[len(self.raw_prefix + "landing/"):].split("/", 1)
            fname, file_id = fname_id[:-len(".json")].rsplit("__", 1)
            self.split(participant, fname, file_id, md["delivery-date"], md["source-file"],
                       md["source-version-id"], body, md["content-sha256"])
            self.stats["files_loaded"] += 1

    def write_run_summary(self, started):
        key = f"{self.raw_prefix}_runs/{started.strftime('%Y%m%dT%H%M%SZ')}.json"
        self._put(key, json.dumps(self.stats, indent=1).encode("utf-8"))


def main():
    if os.environ.get("MELDRX_INGEST_ENABLED", "true").lower() == "false":
        log("ingest_skipped", reason="MELDRX_INGEST_ENABLED=false")
        return 0
    started, t0 = dt.datetime.now(dt.timezone.utc), time.time()
    job = Ingest()
    mode = "rebuild" if os.environ.get("MELDRX_REBUILD", "false").lower() == "true" else "incremental"
    log("ingest_started", mode=mode, source=os.environ["MELDRX_SOURCE_URI"], raw=os.environ["MELDRX_RAW_URI"])
    if mode == "rebuild":
        job.rebuild()
    else:
        job.run()
    job.write_run_summary(started)
    summary = {k: v for k, v in job.stats.items() if k != "per_file"}
    log("ingest_finished", seconds=round(time.time() - t0, 1), **summary)
    return 1 if job.stats["failed"] else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ClientError as exc:
        log("ingest_aborted", error=repr(exc))
        sys.exit(2)
