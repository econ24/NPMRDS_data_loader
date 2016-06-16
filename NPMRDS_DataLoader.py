#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  9 11:32:43 2015

@author: econ24
"""

import sys, os

import DataFormatter, DataUploader, DataExtractor
import BigQuery_Configuration, BigQuery_Append
import Google_Service_Builder
        
def helpFunc(args = None):
    if not args:
        string = \
        """
        
########################################
    
    usage: DataLoader.py [config_file_path] [command] [options]
    commands: help, configure, load
    
    for help on a specific command, type:
    $ DataLoader.py help <command>
    
########################################
        
        """
        print string
        return
        
    command = args[0]
    
    if command == "load":
        string = \
        """
        
########################################
    
    description: loads file located at <source_file_path> into bigquery
    usage: $ DataLoader.py <config_file_path> load <source_file_path> [options]
    
    options:
        -p <value> : use <value> as projectId
        -d <value> : use <value> as datasetId
        -t <value> : use <value> as tableId
        -e <value> : use <value> as clientEmail
        -k <value> : use <value> as keyFile
        -b <value> : use <value> as bucket
    
########################################
        
        """
        print string
        
    elif command == "configure":
        string = \
        """
        
########################################
    
    description: sets various program defaults and saves them to file located at <config_file_path>
    usage: $ DataLoader.py <config_file_path> configure [options]
    
    options:
        -p <value> : set default projectId to <value>
        -d <value> : set default datasetId to <value>
        -t <value> : set default tableId to <value>
        -e <value> : set default clientEmail to <value>
        -k <value> : set default keyFile to <value>
        -b <value> : set default bucket to <value>
    
########################################
        
        """
        print string
#end helpFunc
        
def extractFile(tarball):
    print "<DataLoader> Extracting file from: %s" % tarball
    sourceFile = DataExtractor.extractData(tarball)
    
    formattedFile = "formatted_"+sourceFile
    
    return sourceFile, formattedFile
#end extractFile
    
def loadData(tarball, params):
    sourceFile, formattedFile = extractFile(tarball)
        
    #open and format input file
    print "<DataLoader> formatting file: %s" % sourceFile
    try:
        inFile = open(sourceFile)
    except:
        print "<DataLoader> Could not retrieve data from file: %s" % sourceFile
        return
        
    DataFormatter.formatFile(inFile, formattedFile)
    
    inFile.close()
    
    print "<DataLoader> Successfully formatted file: %s" % sourceFile
            
    #attempt to upload file
    uploaded = DataUploader.upload(formattedFile, params["bucket"], **params)
                        
    if not uploaded:
        print "<DataLoader> Failed to upload file, exiting"
        return
    
    #create bigquery object
    try:
        bigquery = Google_Service_Builder.buildBigQuery(**params)
    except Exception as e:
        print "<DataLoader>", e
        return
        
    source = "gs://"+params["bucket"]+"/"+formattedFile
    BigQuery_Append.appendData(bigquery, source, **params)
#end loadData

def main():
    if len(sys.argv) == 1:
        helpFunc()
        return
        
    if sys.argv[1] == "help":
        helpFunc(sys.argv[2:])
        return
        
    if len(sys.argv) >= 3:
        configFile = sys.argv[1]
        action = sys.argv[2].lower()
        args = sys.argv[3:]
    else:
        return
    
    if action == "configure":
        BigQuery_Configuration.configureBigquery(configFile, args)
        return
        
    if action != "load":
        helpFunc()
        return
    
    #get parameters
    defaults = BigQuery_Configuration.loadConfig(configFile)    
    args, params = BigQuery_Configuration.loadParams(args)
    
    for key in defaults:
        if key not in params:
            params[key] = defaults[key]
    
    #check source
    source = args[0]
    try:
        if os.path.isdir(source):
            print "<DataLoader> Source is a directory"
            for tarball in [f for f in os.listdir(source) if \
                                    os.path.isfile(os.path.join(source, f))]:
                print "<DataLoader> Loading file: %s" % tarball
                loadData(os.path.join(source, tarball), params)
        else:
            loadData(source, params)
        
        DataExtractor.cleanup()
    except Exception as e:
        print e
#end main

if __name__ == "__main__":
    main()