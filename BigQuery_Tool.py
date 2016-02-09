#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4 13:42:11 2015

@author: econ24
"""

import sys, json

import Google_Service_Builder, BigQuery_Configuration, BigQuery_Basics, BigQuery_Append
import DataUploader

def doHelp():
    print \
'''
########################################

    description: tool for executing basic bigQuery commands
    usage: $ BigQuery_Tool.py [config_file_path] [command] [options]

########################################
'''

def main():
    if len(sys.argv) <= 2:
        doHelp()
        return

    elif len(sys.argv) >= 3:
        configFile = sys.argv[1]
        action = sys.argv[2].lower()
        args = sys.argv[3:]

    else:
        return

    if action == "configure":
        BigQuery_Configuration.configureBigquery(configFile, args)
        return

    defaults = BigQuery_Configuration.loadConfig(configFile)

    args, params = BigQuery_Configuration.loadParams(args)

    for key in defaults:
        if key not in params:
            params[key] = defaults[key]

    try:
        bigquery = Google_Service_Builder.buildBigQuery(**params)
    except Exception as e:
        print e
        print "failed to build bigquery"
        return

    if action == "list":
        try:
            BigQuery_Basics.listTables(bigquery, **params)
        except Exception as e:
            print e
            print "failed to list tables in: %s.%s" % (params["projectId"], params["datasetId"])
    elif action == "create":
        try:
            with open(args[0]) as f:
                schema = json.load(f)
            BigQuery_Basics.createTable(bigquery, schema, **params)
        except Exception as e:
            print e
            print "failed to create table: %s" % params["tableId"]
    elif action == "status":
        BigQuery_Basics.getStatus(bigquery, params["projectId"], args[0])
    elif action == "upload":
        if not DataUploader.upload(args[0], params["bucket"]):
            print "<DataLoader> Failed to upload file, exiting"

#end main

if __name__ == "__main__":
    main()
