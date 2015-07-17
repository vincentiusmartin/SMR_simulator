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
from tqdm import *

#===============================================================================================

# Define some constants

# All bytes related variables use bytes as their units

# Notes: flags - write -> 0 ; read -> 1

SECTOR_SIZE = 512 #default 512B

#DISK_SIZE = 9437184 / SECTOR_SIZE #default 1TB-1099511627776
PCACHE_SIZE = 25600 / SECTOR_SIZE #pcache in this script is tantamount to persistent_cache, default 100GB-107374182400
BAND_SIZE = 5120 / SECTOR_SIZE #default 10MB-10485760

# Disk element

current_pcache_idx = 0
pcache_map = []

# Input file

trace = open('in/trace.txt','r');

#===============================================================================================

# Functions

class HaltException(Exception):
    pass

def cleanPCache(time,devno):
    
    global current_pcache_idx
    dirty_band = []
    
    for blkno,blkcount in pcache_map:
        starting_band = int(blkno / BAND_SIZE)
        band_count = int(math.ceil((blkcount + (blkno % BAND_SIZE)) / BAND_SIZE))
        #print(str(starting_band) + "&" + str(band_count) + " <-bandcount, blkno&blkcount-> " + str(blkno) + "&" + str(blkcount))
        for i in range (starting_band, starting_band + band_count):
            dirty_band.append(i)
            
    for band in dirty_band:
        starting_blkno = band * BAND_SIZE + PCACHE_SIZE
        #read
        sys.stdout.write("{} {} {} {} {}\n".format(time, devno, starting_blkno, BAND_SIZE, 1))
        #write
        sys.stdout.write("{} {} {} {} {}\n".format(time, devno, starting_blkno, BAND_SIZE, 0))
    
    #clear pcache
    current_pcache_idx = 0
    del pcache_map[:]
    #print("endclean")
        
def handleWrite(time, devno, blkno, blkcount):

    global current_pcache_idx
    
    #TODO: assign better handle for over-limit case
    if (current_pcache_idx + blkcount > PCACHE_SIZE):
        raise HaltException("write size is larger than persistent cache limit! script terminated")
    
    #write to persistent cache
    sys.stdout.write("{} {} {} {} {}\n".format(time, devno, current_pcache_idx, blkcount, 0))
    #create map
    pcache_map.append([float(blkno),float(blkcount)]) #idx in pcache, original dest, req_size
    #increment persistent cache idx
    current_pcache_idx += blkcount
        
    if (current_pcache_idx >= 0.9 * PCACHE_SIZE):
        cleanPCache(time,devno)

#===============================================================================================

# Main loop

for line in tqdm(trace):
    token = line.split(" ")
    time = token[0]
    devno = token[1]
    blkno = int(token[2].strip())
    blkcount = int(token[3].strip())
    flag = token[4].strip()
    
    if flag == '1': #read
        sys.stdout.write("{} {} {} {} {}\n".format(time, devno, blkno + PCACHE_SIZE, blkcount, flag))
    else: #write
        handleWrite(time,devno,blkno,blkcount)
        

