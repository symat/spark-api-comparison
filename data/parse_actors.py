#!/bin/python3

import re
import json
import sys


pattern = re.compile(r"""(?P<name>[^\t]+?)?\t+    			# name
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
    print("ERROR\nusage: parse_actors.py <input file> <output_path>\n" + s)
    sys.exit(1)

out_part_num = 0
line_num = 0
out_file = open(sys.argv[2]+"/part-%05d" % out_part_num, "w", encoding="utf-8")


stop_line_pattern = "-----------------------------------------------------------------------------"
start_line_pattern = "----			------"
processing = False
actor_name = ""

for line in open(sys.argv[1], "r", encoding="latin-1"):
    if line.startswith(start_line_pattern):
        processing = True
        continue
    if not processing or len(line.strip()) == 0:
        continue
    if line.startswith(stop_line_pattern):
        break
    
    parsed_data = extract_data(line)
    line_num += 1
    if parsed_data[0] is not None:	
        actor_name = parsed_data[0]
        if line_num > 1000000:
            out_file.close()
            out_part_num += 1
            out_file = open(sys.argv[2]+"/part-%05d" % out_part_num, "w", encoding="utf-8")
            line_num = 0
    
    to_print = "%s\t%s\t%s" % (actor_name, parsed_data[1], parsed_data[2]) 
    print(to_print, file = out_file)

out_file.close()
