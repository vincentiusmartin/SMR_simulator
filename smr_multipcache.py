#!/usr/bin/env python
#title           :smr.py
#description     :Simulate smr disk with multiple persistent cache
#author          :Vincentius Martin
#date            :-
#version         :0.1
#usage           :python smr.py
#notes           :-
#python_version  :Python 2 
#precondition    :trace file available
#==============================================================================

# coding: utf-8

import os
import sys
import math
import argparse
from tqdm import *

#===============================================================================================

# Script's arguments
parser = argparse.ArgumentParser()
parser.add_argument("file", help="trace file to process", nargs='?', type=argparse.FileType('r'), default=sys.stdin)
parser.add_argument("-p","--pcsize", help="size of a persistent cache unit", type=int, default="10485760")
parser.add_argument("-b","--bandsize", help="size of band", type=int, default=10485760)
parser.add_argument("-c","--bandcount", help="count of bands that follow a persistent cache", type=int, default=9)
parser.add_argument("-d","--disksize", help="size of disk", type=int, default=1099511627776)
parser.add_argument("-s","--split", help="split the output to 2 traces: w/r to persistent cache and cleanup", action='store_true')
args = parser.parse_args()

#===============================================================================================

#TODO:need more checking using other test cases + run in real traces

# Define some constants

# All bytes related variables use bytes as their units

# Notes: flags - write -> 0 ; read -> 1; last used pcsize 2147483648

# Test mode size: pcache = 25600~50; band = 5120~10; disk = 256000~50

SECTOR_SIZE = 512 #default 512B

DISK_SIZE = args.disksize / SECTOR_SIZE #default 1TB-1099511627776
PCACHE_UNIT = args.pcsize / SECTOR_SIZE #pcache is tantamount to persistent_cache, default 100GB-107374182400
BAND_SIZE = args.bandsize / SECTOR_SIZE #default 10MB-10485760

# Variables

diskset_size = BAND_SIZE * args.bandcount + PCACHE_UNIT
#TODO:compute persistent cache for remainder case
TOTAL_PCACHE = DISK_SIZE // diskset_size * PCACHE_UNIT

# Disk element

current_pcache_idx = 0
pcache_map = []

# Output file

result = open('out/' + str(sys.argv[1]).strip().split('/')[-1].split('.')[0] + '_smrres.txt','w');

result_cleanup = None
if args.split:
    result_cleanup = open('out/' + str(sys.argv[1]).strip().split('/')[-1].split('.')[0] + '_smrcleanup.txt','w');

# Monitoring variables
numberOfClean = 0
writesPutInPCache = 0
sectorsPutInPCache = 0
#averageDirtyBandsPerClean = 0
totalDirtyBands = 0
totalRead = 0

#===============================================================================================

# Functions

class HaltException(Exception):
    pass

def cleanPCache(time,devno):
    result.write("start cleanup\n")
    
    global current_pcache_idx

    dirty_band = set()
    
    for blkno,blkcount in pcache_map:
        starting_band = int(blkno // BAND_SIZE)
        band_count = int(math.ceil((blkcount + (blkno % BAND_SIZE)) / BAND_SIZE))
        
        result.write("bc " + str(band_count) + "\n")
        
        for i in range (starting_band, starting_band + band_count):
            dirty_band.add(i)
            
    for band in sorted(dirty_band):
        starting_blkno = band * BAND_SIZE + ((band * BAND_SIZE) // (BAND_SIZE * args.bandcount) + 1) * PCACHE_UNIT
        if result_cleanup is None:
            #read
            result.write("{} {} {} {} {}\n".format(time, devno, starting_blkno, BAND_SIZE, 1))
            #write
            result.write("{} {} {} {} {}\n".format(time, devno, starting_blkno, BAND_SIZE, 0))
        else:
            #read
            result_cleanup.write("{} {} {} {} {}\n".format(time, devno, starting_blkno, BAND_SIZE, 1))
            #write
            result_cleanup.write("{} {} {} {} {}\n".format(time, devno, starting_blkno, BAND_SIZE, 0)) 
    
    #clear pcache
    current_pcache_idx = 0
    del pcache_map[:]
    
    result.write("end cleanup\n")

def handleRead(time, devno, blkno, blkcount):
    #print "\n" + PCACHE_RATIO + " -- " + str(ratio)
    
    # count starting blkno
    blkno += (blkno // (BAND_SIZE * args.bandcount) + 1) * PCACHE_UNIT
    while blkcount > 0:
        #avoid pcache if needed
        if blkno % (diskset_size) == 0:
            blkno += PCACHE_UNIT

        #start_blk += (blkToBand(start_blk) // ratio[1] + 1) * BAND_SIZE
        blktoread = 0
        nearesttop = (blkno // (diskset_size) + 1) * diskset_size
        #print nearesttop
        #print("blkno " + str(blkno) + "-- nearesttop " + str(nearesttop))
        if nearesttop - blkno >= blkcount:
            blktoread = blkcount
        else:
            blktoread = nearesttop % blkno
        result.write("{} {} {} {} {}\n".format(time, devno, blkno, blktoread, 1))
        blkcount -= blktoread
        blkno += blktoread
        
def handleWrite(time, devno, blkno, blkcount):
    global current_pcache_idx

    #TODO: might need to create better handling for over-limit case
    if (current_pcache_idx + blkcount > TOTAL_PCACHE):
        raise HaltException("write size is larger than persistent cache limit! script terminated")

    #note the request in pcache map
    pcache_map.append([float(blkno),float(blkcount)])
    
    while blkcount > 0:
        write_target = (current_pcache_idx // PCACHE_UNIT) * diskset_size + (current_pcache_idx % PCACHE_UNIT)
        write_size = min(blkcount, PCACHE_UNIT - current_pcache_idx % PCACHE_UNIT)
        #print str(write_target) + "<- target and size ->" + str(write_size)
        #write to persistent cache
        result.write("{} {} {} {} {}\n".format(time, devno, write_target, write_size, 0))
        #update variables
        current_pcache_idx += write_size
        blkcount -= write_size
        
    #if needed, cleanup
    if (current_pcache_idx >= 0.9 * TOTAL_PCACHE):
        cleanPCache(time,devno)

#===============================================================================================

# Main
if __name__ == "__main__":
    #printConfiguration()
    for line in tqdm(args.file):
        token = line.split(" ")
        time = token[0]
        devno = token[1]
        blkno = int(token[2].strip())
        blkcount = int(token[3].strip())
        flag = token[4].strip()
        
        if flag == '1': #read
            handleRead(time, devno, blkno, blkcount)
            #METRICS part - read to disk
            totalRead += 1
        else: #write
            handleWrite(time, devno, blkno, blkcount)
        
    result.close()
    #printSummary()
        

