"""Stand-in for `dbt build` in the test setup: query every meldrx_raw_* table through Athena.

Proves the downstream step runs only after ingest and can read the JSON text with camelCase paths.
Env: MELDRX_RAW_DB, ATHENA_WORKGROUP
"""
import json
import os
import sys
import time

import boto3

athena, glue = boto3.client("athena"), boto3.client("glue")
db, wg = os.environ["MELDRX_RAW_DB"], os.environ["ATHENA_WORKGROUP"]


def query(sql):
    qid = athena.start_query_execution(QueryString=sql, WorkGroup=wg,
                                       QueryExecutionContext={"Database": db})["QueryExecutionId"]
    while (st := athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"])["State"] in ("QUEUED", "RUNNING"):
        time.sleep(1)
    if st["State"] != "SUCCEEDED":
        raise RuntimeError(f"{st['State']}: {st.get('StateChangeReason')} -- {sql}")
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    head = [c.get("VarCharValue") for c in rows[0]["Data"]]
    return [dict(zip(head, [c.get("VarCharValue") for c in r["Data"]])) for r in rows[1:]]


tables = sorted(t["Name"] for p in glue.get_paginator("get_tables").paginate(DatabaseName=db)
                for t in p["TableList"] if t["Name"].startswith("meldrx_raw_"))
for t in tables:
    r = query(f"""SELECT count(*) AS rows_, count(DISTINCT source_file || source_version_id) AS files,
                         count_if(json_extract_scalar(resource_json, '$.meta.lastUpdated') IS NOT NULL) AS camelcase_reads
                  FROM {t}""")[0]
    print(json.dumps({"event": "post_check", "table": t, **r}), flush=True)
print(json.dumps({"event": "post_check_finished", "tables": len(tables)}))
sys.exit(0 if tables else 1)
