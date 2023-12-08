#!/usr/bin/env python
from operator import itemgetter
import sys
import numpy as np

current_key = None
current_vals_list = []
word = None

#Input comes from STDIN
for line in sys.stdin:
# remove leading and trailing whitespace
    line = line.strip()

#Parse the input from mapper
    key, values = line.split(‘\t’, 1)
    list_of_values = values.split(‘,’)
                                  
#Convert to list of strings to list of int
    list_of_values = [int(i) for i in list_of_values]
    if current_key == key:
        current_vals_list.append(list_of_values)
    else:
        if current_key:
            l = len(current_vals_list)+ 1
            b = np.array(current_vals_list)
            meanval = [np.mean(b[0:l,0]),np.mean(b[0:l,1]),
            np.mean(b[0:l,2]), np.mean(b[0:l,3])]
            print ‘%s\t%s’ % (current_key, str(meanval))
        
        current_vals_list = []
        current_vals_list.append(list_of_values)
        current_key = key

#Output the last key if needed
if current_key == key:
    l = len(current_vals_list)+ 1
    b = np.array(current_vals_list)
    meanval = [np.mean(b[0:l,0]),np.mean(b[0:l,1]),
        np.mean(b[0:l,2]), np.mean(b[0:l,3])]
    print ‘%s\t%s’ % (current_key, str(meanval))