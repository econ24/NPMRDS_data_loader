# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4 17:02:44 2015

@author: econ24
"""

import httplib2
from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.discovery import build

def buildService(clientEmail, keyFile, domain, service, version, **kwargs):
    print "<Google_Service_Builder> retrieving credentials..."
    
    with open(keyFile) as f:
        privateKey = f.read()
    
    credentials = SignedJwtAssertionCredentials(clientEmail, privateKey,
        'https://www.googleapis.com/auth/'+domain)
        
    if not credentials or credentials.invalid:
        raise GSBError("Could not retrieve credentials")
        
    print "<Google_Service_Builder> authorizing credentials..."
    
    http = httplib2.Http()
    httpAuth = credentials.authorize(http)
    
    if not httpAuth:
        raise GSBError("Could not authorize credentials")
    
    print "<Google_Service_Builder> building %s object..." % service
    
    bigquery = build(service, version, http=httpAuth)
    
    if not bigquery:
        raise GSBError("Could not successfully build BigQuery")
    
    print "<Google_Service_Builder> %s successfully built!!!" % service
    
    return bigquery
#end buildService
    
def buildBigQuery(*args, **kwargs):
    return buildService(*args, domain="bigquery", service="bigquery", version="v2", **kwargs)

class GSBError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return repr(self.message)
#end DBException