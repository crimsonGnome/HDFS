#!/usr/bin/env python
import sys
#Calculates mean temperature, humidity, light and CO2
# Input data format:
#"2014-04-29 10:15:32",37,44,31,6
#Output:
#"2014-04-29 10:15 [48.75, 31.25, 29.0, 16.5]"
#Input comes from STDIN (standard input)

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    data = line.split(‘,’)
    l=len(data)
    #For aggregation by minute
    key=str(data[0][0:17])
    value=data[1]+‘,’+data[2]+‘,’+data[3]+‘,’+data[4]
    print ‘%s \t%s’ % (key, value)