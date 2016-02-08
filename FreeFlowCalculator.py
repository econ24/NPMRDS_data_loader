# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 17:47:35 2015

@author: econ24
"""

import BigQuery_Configuration, BigQuery_Query
import Google_Service_Builder

datasetId = "HERE_traffic_data"
tableId = "HERE_NY"

def getTMCs(bigquery, **kwargs):
    query = \
    """
    SELECT tmc, travel_time_all AS time, count(*) AS amount 
    FROM [%s.%s] 
    GROUP BY tmc, time;
    """ % (datasetId, tableId)
    
    jobId = BigQuery_Query.makeQuery(bigquery, query, kwargs["projectId"],
                                     datasetId, tableId)
    
    result = BigQuery_Query.getQueryResults(bigquery, kwargs["projectId"], jobId)
    
    totalRows = int(result["totalRows"])
    pageToken = result["pageToken"]
    
    numResults = len(result["rows"])
    print "numResults: %s" % numResults
    
    data = {}
    
    processResult(data, result)
    
    while numResults < totalRows:
        print "getting next page"
        result = BigQuery_Query.getQueryResults(bigquery, kwargs["projectId"],
                                                jobId, pageToken=pageToken)
                                            
        pageToken = result["pageToken"]
        
        numResults += len(result["rows"])
        print "numResults: %s" % numResults
        
        processResult(data, result)
        
    print "total numResults: %s" % numResults
    
def processResult(data, result):
    for row in result["rows"]:
        tmc = row["f"][0]["v"]
        time = int(row["f"][1]["v"])
        count = int(row["f"][2]["v"])
        
        if tmc not in data:
            data[tmc] = []
        for x in range(count):
            data[tmc].append(time)
        
    
def main():
    params = BigQuery_Configuration.loadConfig("config.ini")
    
    try:
        bigquery = Google_Service_Builder.buildBigQuery(**params)
    except Exception as e:
        print "<DataLoader>", e
        return
        
    getTMCs(bigquery, **params)
    
if __name__ == "__main__":
    main()