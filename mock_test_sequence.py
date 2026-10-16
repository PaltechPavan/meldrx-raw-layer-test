"""Replays the guide's exact sequence against mocked AWS and prints the expected counts."""
import os, sys, io, contextlib
from pathlib import Path
sys.path.insert(0, "app")
os.environ.update(AWS_DEFAULT_REGION="us-east-1", AWS_ACCESS_KEY_ID="x", AWS_SECRET_ACCESS_KEY="x",
                  MELDRX_SOURCE_URI="s3://src/patient_access/", MELDRX_RAW_URI="s3://raw/meldrx/",
                  MELDRX_RAW_DB="rawdb", MELDRX_KMS_KEY="alias/test")
import boto3, json
from moto import mock_aws
import meldrx_ingest as mi

def up(s3, rnd):
    for p in sorted(Path("testdata", rnd).glob("*/*.json")):
        s3.put_object(Bucket="src", Key=f"patient_access/{p.parent.name}/{p.name}", Body=p.read_bytes())

def counts(s3):
    c = {}
    for o in s3.list_objects_v2(Bucket="raw", Prefix="meldrx/resources/").get("Contents", []):
        t = o["Key"].split("/")[2]
        c[t] = c.get(t, 0) + len(s3.get_object(Bucket="raw", Key=o["Key"])["Body"].read().splitlines())
    return dict(sorted(c.items()))

def job(label, s3, **env):
    old = {k: os.environ.get(k) for k in env}; os.environ.update(env)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf): rc = mi.main()
    for k, v in old.items():
        if v is None: os.environ.pop(k)
        else: os.environ[k] = v
    fin = [json.loads(l) for l in buf.getvalue().splitlines() if '"ingest_finished"' in l or '"ingest_skipped"' in l][-1]
    keep = {k: fin[k] for k in ("new_versions", "files_loaded", "quarantined", "deletions", "failed") if k in fin}
    print(f"{label:34} rc={rc} {keep} counts={counts(s3)}")

@mock_aws
def main():
    s3, glue, kms = boto3.client("s3"), boto3.client("glue"), boto3.client("kms")
    kms.create_alias(AliasName="alias/test", TargetKeyId=kms.create_key()["KeyMetadata"]["KeyId"])
    for b in ("src", "raw"): s3.create_bucket(Bucket=b)
    s3.put_bucket_versioning(Bucket="src", VersioningConfiguration={"Status": "Enabled"})
    glue.create_database(DatabaseInput={"Name": "rawdb"})
    up(s3, "round1"); job("Step 11 round1", s3)
    job("S1 re-run", s3)
    up(s3, "round2"); job("S2 round2", s3)
    s3.delete_object(Bucket="src", Key="patient_access/TSTPT0000006/20261002T091000Z.json"); up(s3, "round3")
    job("S5 replacement", s3)
    up(s3, "late"); job("S6a failure (bad db)", s3, MELDRX_RAW_DB="does_not_exist")
    job("S6b recovery", s3)
    up(s3, "fix"); job("S7 corrupt file fixed", s3)
    job("S8 kill switch", s3, MELDRX_INGEST_ENABLED="false")
    job("S9 rebuild", s3, MELDRX_REBUILD="true")
    print("quarantine:", [o["Key"] for o in s3.list_objects_v2(Bucket="raw", Prefix="meldrx/quarantine/")["Contents"]])
    print("tables:", sorted(t["Name"] for t in glue.get_tables(DatabaseName="rawdb")["TableList"]))
main()
