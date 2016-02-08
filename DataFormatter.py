# -*- coding: utf-8 -*-
"""
Created on Mon Mar  9 11:48:28 2015

@author: econ24
"""

import re, datetime

WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    
DATA_LABELS = ["tmc", "date", "epoch", "travel_time_all", "travel_time_passenger", "travel_time_truck", "weekday"]

def formatFile(fp, outFileName):
    outFile = open(outFileName, "w")
    outFile.write(",".join(DATA_LABELS)+"\n")
    
    fp.readline()
    
    total = 0
    lines = []
    for line in fp:
        lines.append(formatLine(line))
        
        total += 1
        
        if len(lines) == 1000:
            data = "\n".join(lines)+"\n"
            outFile.write(data)
            lines = []
            
    data = "\n".join(lines)
    outFile.write(data)
    
    print "<DataFormatter> Finished. %d total lines successfully written." % total
    
    outFile.close()

def formatLine(line):
    data = [l.strip() or '0' for l in line.strip().split(",")]
    
    regex = re.compile("(\d\d)(\d\d)(\d\d\d\d)")
    
    date = data[1]
    
    match = regex.match(date)
    
    newDate = match.group(3)+match.group(1)+match.group(2)
    
    data[1] = newDate
    
    weekday = WEEKDAYS[datetime.date(int(match.group(3)), int(match.group(1)), int(match.group(2))).weekday()]
    
    data.append(weekday)
    
    return ",".join(data)