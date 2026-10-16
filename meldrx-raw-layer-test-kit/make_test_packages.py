"""Write small FAKE MeldRx-style packages for the test account (no real data).

    python make_test_packages.py ./testdata

Creates testdata/<round>/<participant>/<yyyyMMddTHHmmssZ>.json. Upload each round when the guide says so.
  round1  normal deliveries: 3 participants with Patient, Encounter, Condition, AllergyIntolerance
  round2  createdAt without offset, duplicate delivery, Patient-only package, new resource type (Procedure with object
          category while AllergyIntolerance uses string-array category), new field, corrupt file
  round3  full package that replaces round2's Patient-only file (the old file is deleted -> delete marker)
  late    a new participant's package, uploaded between runs (used for the failure/recovery test)
  fix     a valid package re-uploaded to the corrupt file's key (a new S3 version of the same file)
"""
import json
import sys
from pathlib import Path


def entry(rtype, rid, extra=None, ts="2026-10-01T10:00:00.1234567Z"):
    r = {"resourceType": rtype, "id": rid, "meta": {"lastUpdated": ts}, **(extra or {})}
    return {"fullUrl": f"https://example.test/fhir/{rtype}/{rid}", "resource": r,
            "response": {"status": "200", "lastModified": ts}}


def patient(pid):
    return entry("Patient", f"pat-{pid}", {"name": [{"family": "Test", "given": [f"Person{pid[-2:]}"]}],
                                           "gender": "unknown", "birthDate": "1970-01-01"})


def full_package(pid, created, extra_entries=()):
    entries = [patient(pid),
               entry("Encounter", f"enc-{pid}-1", {"status": "finished", "period": {"start": "2026-09-01"}}),
               entry("Condition", f"con-{pid}-1", {"code": {"coding": [{"system": "http://hl7.org/fhir/sid/icd-10-cm",
                                                                         "code": "Z00.00"}]}}),
               entry("AllergyIntolerance", f"alg-{pid}-1", {"category": ["food"],
                                                            "code": {"text": "No known allergy"}}),
               *extra_entries]
    return {"package_metadata": {"createdAt": created, "downloadTotal": len(entries) + 6},
            "bundle": {"resourceType": "Bundle", "type": "searchset", "entry": entries}}


def write(root, rnd, pid, fname, doc):
    p = root / rnd / pid / f"{fname}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(doc if isinstance(doc, bytes) else json.dumps(doc, separators=(",", ":")).encode())


def main(out):
    root = Path(out)
    # round 1: normal deliveries
    for i in (1, 2, 3):
        pid = f"TSTPT000000{i}"
        write(root, "round1", pid, "20261001T100000Z", full_package(pid, "2026-10-01T10:00:00+00:00"))
    # round 2: scenarios
    write(root, "round2", "TSTPT0000004", "20261002T090000Z", full_package("TSTPT0000004", "2026-10-02T09:00:00"))
    dup = full_package("TSTPT0000005", "2026-10-02T09:05:00+00:00")
    write(root, "round2", "TSTPT0000005", "20261002T090500Z", dup)           # duplicate delivery:
    write(root, "round2", "TSTPT0000005", "20261002T090507Z", dup)           # same content, two files
    write(root, "round2", "TSTPT0000006", "20261002T091000Z",                 # Patient-only / truncated
          {"package_metadata": {"createdAt": "2026-10-02T09:10:00+00:00", "downloadTotal": 120},
           "bundle": {"resourceType": "Bundle", "type": "searchset", "entry": [patient("TSTPT0000006")]}})
    write(root, "round2", "TSTPT0000007", "20261002T091500Z", full_package(   # new type + shape clash + new field
        "TSTPT0000007", "2026-10-02T09:15:00+00:00",
        [entry("Procedure", "prc-TSTPT0000007-1", {"category": {"coding": [{"code": "387713003"}], "text": "Surgical"},
                                                   "bodySite": [{"text": "Left knee"}]})]))
    write(root, "round2", "TSTPT0000008", "20261002T092000Z", b'{"package_metadata": {"createdAt": ')  # corrupt
    # round 3: replacement of the Patient-only file (same createdAt, as seen in prod)
    write(root, "round3", "TSTPT0000006", "20261003T080000Z", full_package("TSTPT0000006", "2026-10-02T09:10:00+00:00"))
    write(root, "late", "TSTPT0000009", "20261005T000000Z", full_package("TSTPT0000009", "2026-10-05T00:00:00+00:00"))
    write(root, "fix", "TSTPT0000008", "20261002T092000Z", full_package("TSTPT0000008", "2026-10-02T09:20:00+00:00"))
    print(f"wrote test packages under {root.resolve()}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "testdata")
