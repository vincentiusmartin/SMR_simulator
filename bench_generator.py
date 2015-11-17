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
parser.add_argument("-r","--readnum", help="how many reads per iteration", type=int, default=1)
parser.add_argument("-R","--readsize", help="read size in KB", type=int, default=1024)
parser.add_argument("-w","--writenum", help="how many writes per iteration", type=int, default=1)
parser.add_argument("-W","--writesize", help="write size in KB", type=int, default=4)
parser.add_argument("-i","--iter", help="how many iterations", type=int, default=1000)
parser.add_argument("-d","--disksize", help="disk size in GB", type=int, default=500)
args = parser.parse_args()

#==============================================================================

DISK_SIZE = args.disksize * 2097152 #sectors
READ_SIZE = args.readsize * 2 #sectors
WRITE_SIZE = args.writesize * 2 #sectors

#==============================================================================

if __name__ == "__main__":
    for i in range(0,args.iter):
        for x in range(0,args.readnum):
            print("123 1 " + str(randint(0,DISK_SIZE)) + " " + str(READ_SIZE) + " 1")
        for y in range(0,args.writenum):
            print("123 1 " + str(randint(0,DISK_SIZE)) + " " + str(WRITE_SIZE) + " 0")
