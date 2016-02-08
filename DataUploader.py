# -*- coding: utf-8 -*-
"""
Created on Tue Mar 10 12:27:31 2015

@author: econ24
"""

import httplib2, random, time

from apiclient.discovery import build
from apiclient.errors import HttpError
from apiclient.http import MediaFileUpload

from oauth2client.client import SignedJwtAssertionCredentials

import Google_Service_Builder

# Retry transport and file IO errors.
RETRYABLE_ERRORS = (httplib2.HttpLib2Error, IOError)

# Number of times to retry failed downloads.
NUM_RETRIES = 5

# Number of bytes to send/receive in each request.
CHUNKSIZE = 2 * 1024 * 1024

# Mimetype to use if one can't be guessed from the file extension.
DEFAULT_MIMETYPE = 'application/octet-stream'

def buildService(clientEmail, keyFile, **kwargs):
    with open(keyFile) as f:
        privateKey = f.read()
    
    credentials = SignedJwtAssertionCredentials(clientEmail, privateKey,
        'https://www.googleapis.com/auth/devstorage.read_write')
        
    if not credentials or credentials.invalid:
        return None
    
    http = httplib2.Http()
    httpAuth = credentials.authorize(http)
    
    return build('storage', 'v1', http=httpAuth)
#end buildService

def handle_progressless_iter(error, progressless_iters):
    if progressless_iters > NUM_RETRIES:
        print 'Failed to make progress for too many consecutive iterations.'
        raise error

    sleeptime = random.random() * (2**progressless_iters)
    print ('Caught exception (%s). Sleeping for %s seconds before retry #%d.' 
        % (str(error), sleeptime, progressless_iters))
    time.sleep(sleeptime)

def upload(filename, bucketName, clientEmail, keyFile, **kwargs):
    service = Google_Service_Builder.buildService(clientEmail, keyFile, 
                                                  domain="devstorage.read_write",
                                                  service="storage", 
                                                  version="v1", **kwargs)

    print 'Building upload request...'
    media = MediaFileUpload(filename, chunksize=CHUNKSIZE, resumable=True)
    if not media.mimetype():
        media = MediaFileUpload(filename, DEFAULT_MIMETYPE, resumable=True)
    request = service.objects().insert(bucket=bucketName, name=filename,
                                                            media_body=media)

    print 'Uploading file: %s to: %s/%s' % (filename, bucketName, filename)

    progressless_iters = 0
    response = None
    while response is None:
        error = None
        try:
            progress, response = request.next_chunk()
            if progress:
                print 'Upload progress: %.2f%%' % (100.0 * progress.progress())
        except HttpError, err:
            error = err
            if err.resp.status < 500:
                raise
        except RETRYABLE_ERRORS, err:
            error = err

    if error:
        progressless_iters += 1
        handle_progressless_iter(error, progressless_iters)
    else:
        progressless_iters = 0
        
    print "Upload complete"
    
    return True