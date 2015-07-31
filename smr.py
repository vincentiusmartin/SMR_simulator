#!/usr/bin/env python
#title           :smr.py
#description     :Simulate smr disk
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
parser.add_argument("-p","--pcsize", help="size of persistent cache", type=int, default=107374182400)
parser.add_argument("-b","--bandsize", help="size of band", type=int, default=10485760)
parser.add_argument("-s","--split", help="split the output to 2 traces: w/r to persistent cache and cleanup", action='store_true')
args = parser.parse_args()

#===============================================================================================

# Define some constants

# All bytes related variables use bytes as their units

# Notes: flags - write -> 0 ; read -> 1; last used pcsize 2147483648

# Test mode size: pcache = 25600~50; band = 5120~10

SECTOR_SIZE = 512 #default 512B

#DISK_SIZE = 9437184 / SECTOR_SIZE #default 1TB-1099511627776
PCACHE_SIZE = args.pcsize / SECTOR_SIZE #pcache is tantamount to persistent_cache, default 100GB-107374182400
BAND_SIZE = args.bandsize / SECTOR_SIZE #default 10MB-10485760

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
    result.write("startclean")
    global current_pcache_idx
    global numberOfClean
    global totalDirtyBands
    dirty_band = set()
    
    #METRICS part - increment number of clean
    numberOfClean += 1
    
    for blkno,blkcount in pcache_map:
        starting_band = int(blkno / BAND_SIZE)
        band_count = int(math.ceil((blkcount + (blkno % BAND_SIZE)) / BAND_SIZE))
        #print(str(starting_band) + "&" + str(band_count) + " <-bandcount, blkno&blkcount-> " + str(blkno) + "&" + str(blkcount))
        for i in range (starting_band, starting_band + band_count):
            dirty_band.add(i)
            #METRICS part - total dirty band, used for average dirty bands per clean
            totalDirtyBands += 1
          
    for band in sorted(dirty_band):
        starting_blkno = band * BAND_SIZE + PCACHE_SIZE
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
    result.write("endclean")
        
def handleWrite(time, devno, blkno, blkcount):

    global current_pcache_idx
    global writesPutInPCache
    global sectorsPutInPCache
    
    #TODO: assign better handling for over-limit case
    if (current_pcache_idx + blkcount > PCACHE_SIZE):
        raise HaltException("write size is larger than persistent cache limit! script terminated")
    
    #write to persistent cache
    result.write("{} {} {} {} {}\n".format(time, devno, current_pcache_idx, blkcount, 0))
    #METRICS part - writes and sectors put in persistent cache
    writesPutInPCache += 1
    sectorsPutInPCache += blkcount
    #create map
    pcache_map.append([float(blkno),float(blkcount)])
    #increment persistent cache idx
    current_pcache_idx += blkcount
        
    if (current_pcache_idx >= 0.9 * PCACHE_SIZE):
        cleanPCache(time,devno)

def printConfiguration():
    print("------------Configuration------------")
    print("Persistent cache size: " + str(PCACHE_SIZE))
    print("Band size: " + str(BAND_SIZE))
    print("-------------------------------------")

def printSummary():
    print("------------Result Summary------------")
    print("Number of clean: " + str(numberOfClean))
    print("Total writes to persistent cache: " + str(writesPutInPCache))
    print("Total read to disk: " + str(totalRead))
    print("Total sectors to persistent cache: " + str(sectorsPutInPCache))
    if numberOfClean > 0:
        print("Averages dirty bands per clean: " + str(float(totalDirtyBands) / numberOfClean));
    print("--------------------------------------")

#===============================================================================================

# Main
if __name__ == "__main__":
    printConfiguration()
    for line in tqdm(args.file):
        token = line.split(" ")
        time = token[0]
        devno = token[1]
        blkno = int(token[2].strip())
        blkcount = int(token[3].strip())
        flag = token[4].strip()
        
        if flag == '1': #read
            result.write("{} {} {} {} {}\n".format(time, devno, blkno + PCACHE_SIZE, blkcount, flag))
            #METRICS part - read to disk
            totalRead += 1
        else: #write
            handleWrite(time,devno,blkno,blkcount)
        
    result.close()
    printSummary()
        

