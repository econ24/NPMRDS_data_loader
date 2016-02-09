# -*- coding: utf-8 -*-
"""
Created on Thu Mar  5 15:33:26 2015

@author: econ24
"""

def appendData(bigquery, source, projectId, datasetId, tableId, **kwargs):
    body = {
        "configuration": {
            "load": {
                "destinationTable": {
                    "datasetId": datasetId,
                    "projectId": projectId,
                    "tableId": tableId
                },
                "skipLeadingRows": 1,
                "sourceFormat": "CSV",
                "sourceUris": [source],
                "writeDisposition": "WRITE_APPEND"
            }
        }
    }
    job = bigquery.jobs().insert(projectId=projectId, body=body).execute()

    state = job["status"]["state"].lower()
    jobId = job["jobReference"]["jobId"]

    while state != "done":
        time.sleep(15)
        job = bigquery.jobs().get(projectId=projectId, jobId=jobId).execute()
        state = job["status"]["state"].lower()
        print "<BigQuery> Job status: %s" % state

    print job
#end appendData

def insertData(bigquery, source, projectId, datasetId, tableId, **kwargs):
    body = {
        "configuration": {
            "load": {
                "destinationTable": {
                    "datasetId": datasetId,
                    "projectId": projectId,
                    "tableId": tableId
                },
                "skipLeadingRows": 1,
                "sourceFormat": "CSV",
                "sourceUris": [source],
                "writeDisposition": "WRITE_TRUNCATE"
            }
        }
    }
    job = bigquery.jobs().insert(projectId=projectId, body=body).execute()

    state = job["status"]["state"].lower()
    jobId = job["jobReference"]["jobId"]

    while state != "done":
        time.sleep(15)
        job = bigquery.jobs().get(projectId=projectId, jobId=jobId).execute()
        state = job["status"]["state"].lower()
        print "<BigQuery> Job status: %s" % state

    print job
#end insertData

"""
###################
WORK IN PROGRESS!!!
###################
"""

import time

def appendFile(bigquery, source, **kwargs):
    with open(source) as f:
        schema = [d.strip() for d in f.readline().strip().split(",")]
        records = []
        for line in f:
            data = [d.strip() for d in line.strip().split(",")]
            record = {}
            for i in range(len(schema)):
                record[schema[i]] = data[i]
            records.append(record)

            if (len(records) >= 25):
                streamRecords(bigquery, records, 0, **kwargs)

        if len(records):
            streamRecords(bigquery, records, **kwargs)
#end appendFile

def streamRecords(bigquery, records, attempt, projectId, datasetId, tableId, insertId=None, **kwargs):
    if attempt == 3:
        print "could not upload:"
        for record in records:
            print record
        return

    insertId = insertId or "insertId-"+str(time.time())

    body = {
        "kind": "bigquery#tableDataInsertAllRequest",
        "insertId": insertId,
        "rows": []
    }

    attempts = []

    while len(records):
        record = records.pop()
        body["rows"].append({"json": record})
        attempts.append(record)

    try:
        response = bigquery.tabledata().insertAll(projectId=projectId, datasetId=datasetId, tableId=tableId, body=body).execute()
    except:
        streamRecords(bigquery, attempts, attempt+1, projectId, datasetId, tableId, insertId)
        return

    if response.get("insertErrors"):
        errors = []
        for error in response["insertErrors"]:
            index = error["index"]
            errors.append(attempts[index])
        streamRecords(bigquery, records, attempt+1, projectId, datasetId, tableId)
#end streamRecords
