# -*- coding: utf-8 -*-
"""
Created on Thu Mar  5 15:08:13 2015

@author: econ24
"""

def listTables(bigquery, projectId, datasetId, **kwargs):
    tableList = bigquery.tables().list(projectId=projectId, datasetId=datasetId).execute()

    for table in tableList["tables"]:
        print table["id"]

def createTable(bigquery, schema, tableId, projectId, datasetId, **kwargs):
    body = {
        "schema": { "fields": schema },
        "tableReference": {
            "projectId": projectId,
            "tableId": tableId,
            "datasetId": datasetId
        }
    }

    bigquery.tables().insert(body=body, projectId=projectId, datasetId=datasetId).execute()

def deleteTable(bigquery, tableId, projectId, datasetId, **kwargs):
    bigquery.tables().delete(projectId=projectId, datasetId=datasetId, tableId=tableId).execute()

def getStatus(bigquery, projectId, jobId):
    print bigquery.jobs().get(projectId=projectId, jobId=jobId).execute()
