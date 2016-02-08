# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 19:18:35 2015

@author: econ24
"""

import time

def makeQuery(bigquery, query, projectId, datasetId, tableId, **kwargs):
    body = {
        "configuration": {
            "query": {
                "query": query
            }
        }
    }
    job = bigquery.jobs().insert(projectId=projectId, body=body).execute()
    
    state = job["status"]["state"].lower()
    jobId = job["jobReference"]["jobId"]
    
    while state != "done":
        time.sleep(2)
        job = bigquery.jobs().get(projectId=projectId, jobId=jobId).execute()
        state = job["status"]["state"].lower()
        print "<BigQuery> Job status: %s" % state
            
    return jobId
            
def getQueryResults(bigquery, projectId, jobId, pageToken=None, maxResults=None, startIndex=None, **kwargs):
    return bigquery.jobs().getQueryResults(projectId=projectId, 
                                            jobId=jobId, 
                                            pageToken=pageToken, 
                                            startIndex=startIndex, 
                                            maxResults=maxResults).execute()
                                            
def getAllQueryResults(bigquery, projectId, jobId, pageToken=None, maxResults=None, startIndex=None, **kwargs):
    result = bigquery.jobs().getQueryResults(projectId=projectId, 
                                            jobId=jobId, 
                                            pageToken=pageToken, 
                                            startIndex=startIndex, 
                                            maxResults=maxResults).execute()
        
    pageToken = result["pageToken"]
        
    while len(result["rows"]) < result["totalRows"]:
        nextResult = bigquery.jobs().getQueryResults(projectId=projectId, 
                                            jobId=jobId, 
                                            pageToken=pageToken).execute()
                                            
        pageToken = result["pageToken"]
        
        result["rows"].extend(nextResult["rows"]) 
    
    return result
                                            
def processResult(result):
    schema = [{f["name"]:f["type"]} for f in result["schema"]["fields"]]
    
    rows = [[attr["v"] for attr in row["f"]] for row in result["rows"]]
    
    return {
        "schema": schema,
        "rows": rows
    }