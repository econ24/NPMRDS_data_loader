# -*- coding: utf-8 -*-
"""
Created on Thu Mar  5 13:19:40 2015

@author: econ24
"""

import json

DEFAULTS_CONFIG_MAP = {
    "-p": "projectId",
    "-d": "datasetId",
    "-t": "tableId",
    "-e": "clientEmail",
    "-k": "keyFile",
    "-b": "bucket"
}
OPTIONS_CONFIG_MAP = {
    "-s": "source"
}

def configureBigquery(configFile, args):
    try:
        f = open(configFile)
        config = json.load(f)
    except:
        f = None
        config = {}
    
    for x in range(0, len(args), 2):
        attribute = DEFAULTS_CONFIG_MAP.get(args[x])
            
        if attribute:
            config[attribute] = args[x+1]
        
    with open(configFile, "w") as f:
        json.dump(config, f)
    
def loadConfig(configFile):
    try:
        f = open(configFile)
        return json.load(f)
    except:
        return {}
        
def loadParams(args):
    params = {}
    
    x = 0
    while x < len(args):
        attribute = DEFAULTS_CONFIG_MAP.get(args[x]) or OPTIONS_CONFIG_MAP.get(args[x])
            
        if attribute:
            params[attribute] = args[x+1]
            args[x] = "None"
            args[x+1] = "None"
            x += 1
        x += 1
            
    args = [a for a in args if a != "None"]
            
    return args, params
    