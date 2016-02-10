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
# end appendData

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
# end insertData

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
        # end for loop

        count = 0
        step = 50
        while count < len(records):
            streamRecords(bigquery, records[count : count + step], **kwargs)
            count += step
# end appendFile

def streamRecords(bigquery, records, projectId, datasetId, tableId, **kwargs):

    body = {
        "kind": "bigquery#tableDataInsertAllRequest",
        "rows": []
    }

    for record in records:
        body["rows"].append({ "json": record })

    try:
        response = bigquery.tabledata().insertAll(projectId=projectId, datasetId=datasetId, tableId=tableId, body=body).execute()
    except:
        failedRecords(records)
        return

    if response.get("insertErrors"):
        errors = []
        for error in response["insertErrors"]:
            index = error["index"]
            errors.append(records[index])
        failedRecords(errors)
#end streamRecords

def failedRecords(records):
    print "<BigQuery_Append.streamRecords> Failed to append records..."
    for record in errors:
        print record
