#!/usr/bin/env python
#title           :bench_generator.py
#description     :Generate custom benchmark
#author          :Vincentius Martin
#date            :-
#version         :0.1
#usage           :usage here
#==============================================================================

from random import randint
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-r","--readsize", help="read size in KB", type=int, default=1024)
parser.add_argument("-w","--writesize", help="write size in KB", type=int, default=4)
parser.add_argument("-t","--itertime", help="time in sec per 1 iteration", type=float, default=1)
parser.add_argument("-n","--numwrites", help="number of write every 1 iteration", type=int, default=2)
parser.add_argument("-i","--iter", help="how many iterations", type=int, default=100)
parser.add_argument("-d","--disksize", help="disk size in GB", type=int, default=800)
args = parser.parse_args()

#==============================================================================

DISK_SIZE = args.disksize * 2097152 #sectors
READ_SIZE = args.readsize * 2 #sectors
WRITE_SIZE = args.writesize * 2 #sectors
iter_time = args.itertime * 1000
time = 0.0;
readtime = 10

#==============================================================================

if __name__ == "__main__":
    for i in range(0,args.iter):
        print(str(readtime) + " 0 " + str(randint(0,DISK_SIZE)) + " " + str(READ_SIZE) + " 1")
        readtime += 1000
        for y in range(0,args.numwrites):
            time += round((iter_time / args.numwrites),3)
            print(str(time) + " 0 " + str(randint(0,DISK_SIZE)) + " " + str(WRITE_SIZE) + " 0")
            
