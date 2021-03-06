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
parser.add_argument("-l","--logsize", help="size of a persistent cache log", type=int, default="10485760")
#parser.add_argument("-t","--pctotal", help="total size of the whole persistent cache", type=int, default="10737418240")
parser.add_argument("-g","--group", help="every n group size", type=int, default="104857600")
parser.add_argument("-b","--bandsize", help="size of band", type=int, default=10485760)
parser.add_argument("-d","--disksize", help="size of disk", type=int, default=1099511627776)
parser.add_argument("-p","--policy", help="A,B,C,shelter", type=str, default="A")
parser.add_argument("-n","--noclean", help="disable clean", action='store_true')
parser.add_argument("-s","--split", help="split the output to 2 traces: w/r to persistent cache and cleanup", action='store_true')
args = parser.parse_args()

#===============================================================================================

# Define some constants

#KB = 1024
#MB = 1024 * KB
#GB = 1024 * MB

# All bytes related variables use bytes as their units

# Notes: flags - write -> 0 ; read -> 1; last used pcunit 2147483648

# Policy Notes:
# 1. POL-A lastTail=latest read - many caches clean
# 2. POL-B write W to its nearest band's log - single cache clean
# 3. POL-C lastTail = latest non-logged I/O (all reads OR big writes)
# 4. Sheltering, small write go to shelter near last tail (offset + size) of the latest big IO

# Test mode size: pcache = 25600~50; band = 5120~10; disk = 256000~500
# Test mode: python smr_multipcache.py in/trace2.txt -l 5120 -g 51200 -b 5120 -d 256000
# Real mode: python smr_multipcache.py in/disk4_t10.txt -u 106954752 -t 10737418240 -b 41943040 -d 107374182400

SECTOR_SIZE = 512 #default 512B

# Constants
DISK_SIZE = args.disksize // SECTOR_SIZE #default 1TB-1099511627776
PCACHE_SIZE = args.logsize // SECTOR_SIZE #tantamount to persistent_cache, default 100GB-107374182400
BAND_SIZE = args.bandsize // SECTOR_SIZE #default 10MB-10485760
TOTAL_PCACHE = ((args.disksize // args.group) * args.logsize) / 512 #default 10GB - 10737418240
SMALL_IO_SIZE = 32 #in KB

band_unit = (args.group - args.logsize) // args.bandsize;
diskset_size = args.group // SECTOR_SIZE
last_tail = 0 #last tail depends on policy -- (offset + size)

# Disk element
pcache = [list() for _ in xrange(DISK_SIZE // diskset_size)]
log_swap_idx = int(0.8 * len(pcache)) #put log in 90% of the disk

# Output file
result = open('out/' + str(sys.argv[1]).strip().split('/')[-1].split('.')[0] + '_smrmultires.txt','w');

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

def computeDiskBlkNo(blkno): #basically, this function means we add with the size of n PCACHE_SIZE before
    return blkno + (blkno // (BAND_SIZE * band_unit) + 1) * PCACHE_SIZE

# --------End of Computation Functions--------



# --------Start of Read,Write,Clean--------

def logSwap(time,devno,punit_idx):
    global log_swap_idx

    while len(pcache[log_swap_idx]) > 0:
        log_swap_idx += 1

    #read the log
    result.write("{} {} {} {} {}\n".format(time, devno, punit_idx * diskset_size, PCACHE_SIZE, 1))
        
    #do the swap! - read swap target
    result.write("{} {} {} {} {}\n".format(time, devno, log_swap_idx * diskset_size, PCACHE_SIZE, 1))
    #do the swap! - write swap target
    result.write("{} {} {} {} {}\n".format(time, devno, log_swap_idx * diskset_size, PCACHE_SIZE, 0))
    
    #move the data to new log
    pcache[log_swap_idx] = pcache[punit_idx]
    del pcache[punit_idx][:] 

#idx = -1 = whole
def cleanPCache(time,devno,punit_idx = -1):
    global totalDirtyBands
    global pcache
    
    dirty_band = set()
    
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
        starting_blkno = band * BAND_SIZE + (band // band_unit + 1) * PCACHE_SIZE
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
    else: #single cleanlog_occupancy.txt
        del pcache[punit_idx][:]

def handleRead(time, devno, blkno, blkcount):
    global last_tail
    global totalRead
   
    #METRICS part - increment total read
    totalRead += 1
    
    #start of the read part
    blkno = computeDiskBlkNo(blkno)
    result.write("{} {} {} {} {}\n".format(time, devno, blkno, blkcount, 1))
    #end of the read part
    
    #if policy A or (shelter and bigIO), save the tail
    if args.policy == "A" or args.policy == "C" or (args.policy == "shelter" and blkcount * 0.5 > SMALL_IO_SIZE):
        last_tail = (blkcount + blkno)
       
def handleDefaultWrite(time, devno, blkno, blkcount):
#DEFAULT: write goes to the persistent cache / log
    global writesPutInPCache
    global sectorsPutInPCache
    global pcache
    global last_tail
    global numberOfClean
    
    #METRICS part - writes and sectors put in persistent cache
    writesPutInPCache += 1
    sectorsPutInPCache += blkcount

    idx_point = -1 #unassigned
    if args.policy == "B":
        idx_point = computeDiskBlkNo(blkno)
    else: #policy A or policy C or shelter
        idx_point = last_tail  

    #clean or reset if not enough space
    if nextIdxPCacheN(idx_point // diskset_size) + blkcount > PCACHE_SIZE:
        #if blkcount > PCACHE_SIZE:
        #    raise HaltException("write size " + str(int(blkcount * 0.5)) + "KB is larger than the log size! script terminated") 
        #METRICS part - increment number of clean
        numberOfClean += 1
        if args.noclean: #NoClean
            #del pcache[idx_point // diskset_size][:]
            logSwap(time,devno,idx_point // diskset_size)
        else: #do cleanup!
            if args.policy == "B":
                cleanPCache(time,devno,idx_point // diskset_size)     
            else: #args.policy == "A" or "C" or shelter
                cleanPCache(time,devno)
    
    #start of the write part
    write_target = (idx_point // diskset_size) * diskset_size + nextIdxPCacheN(idx_point // diskset_size)

    result.write("{} {} {} {} {}\n".format(time, devno, write_target, blkcount, 0))
    pcache[idx_point // diskset_size].append([float(blkno),float(blkcount)])
    #end of the write part


def handleShelterWrite(time, devno, blkno, blkcount):
    global last_tail

    if (blkcount * 0.5) <= SMALL_IO_SIZE: #small request, use pcache
        handleDefaultWrite(time, devno, blkno, blkcount)
    else: #bigIO, this part writes do not go to log
        #basically, just copy paste from read and some trivial changes
        blkno = computeDiskBlkNo(blkno)
        result.write("{} {} {} {} {}\n".format(time, devno, blkno, blkcount, 0))
        #end of the write part
        if args.policy == "C": 
            last_tail = (blkcount + blkno)

# --------End of Read,Write,Clean--------

# --------Start of Print Sectors to Log--------

def printSectorsToLog():
    target = open("out/" + str(sys.argv[1]).strip().split('/')[-1].split('.')[0] + "-log_occupancy.txt",'w')
    for mtl_elm in pcache:
        target.write("%s\n" % int(sum(row[1] for row in mtl_elm) * 0.5)) #in KB
    target.close()

# --------End of Print Sectors to Log--------

# --------Start of User Messages--------
        
def printConfiguration():
    print("------------Configuration------------")
    print("Persistent cache size: " + "%.3f" % (float(TOTAL_PCACHE * SECTOR_SIZE) / 1048576) + " MB")
    print("Band size: " + "%.3f" % (float(BAND_SIZE * SECTOR_SIZE) / 1048576) + " MB")
    print("Bands that follow a persistent cache unit: " + str(band_unit))
    print("Total shelters: " + str(DISK_SIZE / diskset_size))
    print("-------------------------------------")

def printSummary():
    print("------------Result Summary------------")
    print("Number of clean or number of log swap: " + str(numberOfClean))
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
            if args.policy == "shelter" or args.policy == "C":
                handleShelterWrite(time, devno, blkno, blkcount)
            else: #policy A or B
                handleDefaultWrite(time, devno, blkno, blkcount)
        
    result.close()
    printSummary()
    printSectorsToLog()
    
