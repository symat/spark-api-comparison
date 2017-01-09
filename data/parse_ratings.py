#!/bin/python3

import re
import json
import sys


pattern = re.compile(r"""\s*                                   # starting whitespaces
                         (?P<distribution>[\d\.\*]{10}?)\s+    	# distribution of votes
                         (?P<votes>[\d]*?)\s+             	   # number of votes
                         (?P<rating>\d0?\.\d?)\s+             	   # rating
                         ['"]?(?P<title>.+?)['"]?\s*    # movie title, optionally quoted
                         \((?P<year>[\d\?]{4}?).*\)   			# year
                         .*""", re.VERBOSE)			    # anything else
							 							 
def extract_data(s):
    try:
        return pattern.search(s).groups()
    except AttributeError: 
        print("ERROR while parsing the following line\n" + s)
        sys.exit(1)
    return None


if len(sys.argv) != 3:
    print("ERROR\nusage: parse_ratings.py <input file> <output_file>\n" + s)
    sys.exit(1)


stop_line_pattern = "-------------------------------------------------------"
start_line_pattern = "New  Distribution  Votes  Rank  Title"
processing = 0

out_part_num = 0
line_num = 0
out_file = open(sys.argv[2], "w", encoding="utf-8")

for line in open(sys.argv[1], "r", encoding="latin-1"):
    if line.startswith(start_line_pattern):
        processing += 1
        continue
    if processing<3 or len(line.strip()) == 0:
        continue
    if line.startswith(stop_line_pattern):
        break
    
    if "{" in line:
        continue

    parsed_data = extract_data(line)
    to_print = "%s\t%s\t%s\t%s\t%s" % parsed_data
    print(to_print, file = out_file)
    line_num += 1

out_file.close()
print("exported lines: " + line_num)