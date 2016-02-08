# -*- coding: utf-8 -*-
"""
Created on Thu Mar 12 12:57:31 2015

@author: econ24
"""

import subprocess, re

def extractData(tarball):
    args = ["tar", "-tf", tarball]    
    archive = subprocess.check_output(args).split()[-1]
    
    args = ["tar", "-xf", tarball]
    subprocess.call(args)
    
    args = ["unzip", "-l", archive]
    substrings = subprocess.check_output(args).split()
    regex = re.compile("_NY_")
    for substring in substrings:
        if regex.search(substring):
            fileName = substring
            break
    
    args = ["unzip", "-o", archive, fileName]
    
    with open(fileName, "w") as outFile:
        subprocess.call(args, stdout=outFile)
        
    return fileName
    
def cleanup():
    args = ["make", "clean"]
    subprocess.call(args)
    
    args = ["make", "move"]
    subprocess.call(args)