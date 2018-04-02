#!/usr/bin/env python
"""mapper.py  - code adapted from in class lecture which was adapted from http://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/"""

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split(',')
    # increase counters
    if words[0] != "DATE" and len(words) > 27: 
        # do not get the header row and do not get it on a bad row with less then 27 fields
        for word in words[-5:]:
            # write the results to STDOUT (standard output);
            # what we output here will be the input for the
            # Reduce step, i.e. the input for reducer.py
            #
            # tab-delimited; the trivial word count is 1
            if len(word) > 0: # dont do this fore the empty string
                print '%s\t%s' % (word, 1)