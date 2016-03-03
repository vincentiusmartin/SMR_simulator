#!/usr/bin/env python
#title           :superfast.py
#description     :Reprocess all inputs to be between offset 0 and 15 MB
#author          :Vincentius Martin
#==============================================================================

# coding: utf-8

import argparse
import sys
import random
#==============================================================================
parser = argparse.ArgumentParser()
parser.add_argument("file", help="trace file to process", nargs='?', type=argparse.FileType('r'), default=sys.stdin)
args = parser.parse_args()
#==============================================================================
KB = 1024;
MB = 1024 * KB;
SECTOR_SIZE = 512

max_offset = (80 * MB) / SECTOR_SIZE;

result = open('out/' + str(sys.argv[1]).strip().split('/')[-1].split('.')[0] + '_superfast.trace','w');
#==============================================================================

if __name__ == "__main__":
    for line in args.file:
        token = line.split(" ")
        offset = str(random.randint(0, max_offset - int(token[3].strip())))
        result.write("{} {} {} {} {}\n".format(token[0], token[1], offset, token[3].strip(), token[4].strip()))
        
        
