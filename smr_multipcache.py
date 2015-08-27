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
parser.add_argument("-u","--pcunit", help="size of a persistent cache unit", type=int, default="10485760")
parser.add_argument("-t","--pctotal", help="total size of the whole persistent cache", type=int, default="10737418240")
parser.add_argument("-b","--bandsize", help="size of band", type=int, default=10485760)
parser.add_argument("-d","--disksize", help="size of disk", type=int, default=1099511627776)
parser.add_argument("-p","--policy", help="A,B,shelter", type=str, default="A")
parser.add_argument("-n","--noclean", help="disable clean", action='store_true')
parser.add_argument("-s","--split", help="split the output to 2 traces: w/r to persistent cache and cleanup", action='store_true')
args = parser.parse_args()

#===============================================================================================

# Define some constants

# All bytes related variables use bytes as their units

# Notes: flags - write -> 0 ; read -> 1; last used pcunit 2147483648

# Policy Notes:
# 1. POL-A lastTail=latest read - many caches clean
# 2. POL-B write W to its nearest band's log - single cache clean
# 3. Sheltering, small write go to shelter near last tail (offset + size) of the latest big IO

# Test mode size: pcache = 25600~50; band = 5120~10; disk = 256000~500
# Test mode: python smr_multipcache.py in/trace2.txt -u 5120 -t 25600 -b 5120 -d 256000
# Real mode: python smr_multipcache.py in/disk4_t10.txt -u 106954752 -t 10737418240 -b 41943040 -d 107374182400

SECTOR_SIZE = 512 #default 512B

# Constants
DISK_SIZE = args.disksize // SECTOR_SIZE #default 1TB-1099511627776
PCACHE_UNIT = args.pcunit // SECTOR_SIZE #pcache is tantamount to persistent_cache, default 100GB-107374182400
BAND_SIZE = args.bandsize // SECTOR_SIZE #default 10MB-10485760
TOTAL_PCACHE = args.pctotal // SECTOR_SIZE #default 10GB - 10737418240
SMALL_IO_SIZE = 32 #in KB

# Variables
diskset_size = DISK_SIZE // (TOTAL_PCACHE // PCACHE_UNIT) #give temporary value first
band_unit = (diskset_size - PCACHE_UNIT) // BAND_SIZE #how many bands follow a persistent cache
diskset_size = BAND_SIZE * band_unit + PCACHE_UNIT #size of persistent cache + bands in a set before the next pcache
last_tail = 0 #last tail depends on policy -- (offset + size)

#diskset_size = BAND_SIZE * band_count + PCACHE_UNIT
#need to compute persistent cache for remainder case
#TOTAL_PCACHE = DISK_SIZE // diskset_size * PCACHE_UNIT

# Disk element
pcache = [list() for _ in xrange(DISK_SIZE // diskset_size)]

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

# --------Start of Computation Functions--------

def nextIdxPCacheN(n):
    return int(sum(io[1] for io in pcache[n]))

def computeDiskBlkNo(blkno): #basically, this function means we add with the size of n pcache_unit before
    return blkno + (blkno // (BAND_SIZE * band_unit) + 1) * PCACHE_UNIT

# --------End of Computation Functions--------

# --------Start of Read,Write,Clean--------

#idx = -1 = whole
def cleanPCache(time,devno,punit_idx = -1):
    global numberOfClean
    global totalDirtyBands
    global pcache
    
    dirty_band = set()
    #METRICS part - increment number of clean
    numberOfClean += 1
    
    if punit_idx == -1: #whole clean
        for punit in pcache:
            for blkno,blkcount in punit:
                starting_band = int(blkno // BAND_SIZE)
                band_count = int(math.ceil((blkcount + (blkno % BAND_SIZE)) / BAND_SIZE))
                #print "\n"+str(blkno) + "--" + str(blkcount) + "--"+str(starting_band) + "--" + str(band_count)
                for i in range (starting_band, starting_band + band_count):
                    dirty_band.add(i)    
                    #METRICS part - total dirty band, used for average dirty bands per clean
                    totalDirtyBands += 1  
    else: #single clean
        for blkno,blkcount in pcache[punit_idx]:
            starting_band = int(blkno // BAND_SIZE)
            band_count = int(math.ceil((blkcount + (blkno % BAND_SIZE)) / BAND_SIZE))
            for i in range (starting_band, starting_band + band_count):
                dirty_band.add(i)    
                #METRICS part - total dirty band, used for average dirty bands per clean
                totalDirtyBands += 1 
                
    for band in sorted(dirty_band):
        starting_blkno = band * BAND_SIZE + ((band * BAND_SIZE) // (BAND_SIZE * band_unit) + 1) * PCACHE_UNIT
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
    if punit_idx == -1: #whole clean
        pcache = [list() for _ in xrange(DISK_SIZE // diskset_size)]
    else: #single clean
        del pcache[punit_idx][:]

def handleRead(time, devno, blkno, blkcount):
    global last_tail
    global totalRead
    
    #save for tail
    init_blkcount = blkcount
    
    #METRICS part - increment total read
    totalRead += 1
    
    #count starting blkno by skipping persistent caches
    blkno = computeDiskBlkNo(blkno)
    while blkcount > 0:
        #avoid a pcache if the request is on the edge
        if blkno % (diskset_size) == 0:
            blkno += PCACHE_UNIT

        blktoread = 0
        nearesttop = (blkno // (diskset_size) + 1) * diskset_size

        if nearesttop - blkno >= blkcount:
            blktoread = blkcount
        else:
            blktoread = nearesttop % blkno
        result.write("{} {} {} {} {}\n".format(time, devno, blkno, blktoread, 1))
        blkcount -= blktoread
        blkno += blktoread
    
    #if policy A or (shelter and bigIO), save the tail
    if args.policy == "A" or (args.policy == "shelter" and init_blkcount * 0.5 > SMALL_IO_SIZE):
        last_tail = blkno - 1
       
def handleDefaultWrite(time, devno, blkno, blkcount):
    global writesPutInPCache
    global sectorsPutInPCache
    global pcache

    #METRICS part - writes and sectors put in persistent cache
    writesPutInPCache += 1
    sectorsPutInPCache += blkcount

    idx_point = -1 #unassigned
    if args.policy == "B":
        idx_point = computeDiskBlkNo(blkno)
    else: #policy A or shelter
        idx_point = last_tail

    #TODO: if needed do the overlimit case here
    
    while blkcount > 0:
        write_target = -1 #unassigned
        write_target = (idx_point // diskset_size) * diskset_size + nextIdxPCacheN(idx_point // diskset_size)
            
        write_size = min(blkcount, PCACHE_UNIT - nextIdxPCacheN(idx_point // diskset_size))
        #write to pcache
        result.write("{} {} {} {} {}\n".format(time, devno, write_target, write_size, 0))
        pcache[idx_point // diskset_size].append([float(blkno),float(write_size)])
        #---------------
        blkcount -= write_size
        blkno += write_size
        
        if nextIdxPCacheN(idx_point // diskset_size) == PCACHE_UNIT:
            if args.noclean: #NoClean just imagine the log is like a "circular buffer"
                del pcache[idx_point // diskset_size][:] #basically this is tantamount to reset the offset to zero
            else: #do cleanup!
                if args.policy == "B":   
                    cleanPCache(time,devno,idx_point // diskset_size)     
                else: #args.policy == "A" or shelter
                    cleanPCache(time,devno)

def handleShelterWrite(time, devno, blkno, blkcount):
    global last_tail

    if (blkcount * 0.5) <= SMALL_IO_SIZE: #small request, use pcache
        handleDefaultWrite(time, devno, blkno, blkcount)
    else: #bigIO
        #basically, just copy paste from read and some trivial changes
        blkno = computeDiskBlkNo(blkno)
        while blkcount > 0:
            #avoid a pcache if the request is on the edge
            if blkno % (diskset_size) == 0:
                blkno += PCACHE_UNIT

            blktowrite = 0
            nearesttop = (blkno // (diskset_size) + 1) * diskset_size

            if nearesttop - blkno >= blkcount:
                blktowrite = blkcount
            else:
                blktowrite = nearesttop % blkno
            result.write("{} {} {} {} {}\n".format(time, devno, blkno, blktowrite, 0))
            blkcount -= blktowrite
            blkno += blktowrite

        #shelter, bigIO, we know we should save the tail :)
        last_tail = blkno - 1

# --------End of Read,Write,Clean--------

# --------Start of User Messages--------
        
def printConfiguration():
    print("------------Configuration------------")
    print("Persistent cache size: " + str(TOTAL_PCACHE)) + " sectors"
    print("Band size: " + str(BAND_SIZE)) + " sectors"
    print("Bands that follow a persistent cache unit: " + str(band_unit))
    print("Total shelters: " + str(DISK_SIZE / diskset_size))
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
    
# --------End of User Messages--------

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
            handleRead(time, devno, blkno, blkcount)
        else: #write
            if args.policy == "shelter":
                handleShelterWrite(time, devno, blkno, blkcount)
            else: #policy A or B
                handleDefaultWrite(time, devno, blkno, blkcount)
        
    result.close()
    printSummary()
    
