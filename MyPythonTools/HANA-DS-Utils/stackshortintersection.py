#!/usr/bin/env python3
# vim: tabstop=4 shiftwidth=4
import sys
import argparse
from hanads.stackshortutils import dedupStackShort,findSameThread

class __Conf:
    def __init__(self):
        parser = argparse.ArgumentParser(description="Find the threads whose stack didn't change over multiple RTE dump")

        parser.add_argument('rte', nargs='+', help='Input RTE dump files to analyze')
        parser.add_argument('-o','--output', required=False, type=str, help='Output to file. If not given ouput will be written to stdout')
        parser.add_argument('-s','--span', required=False, type=int, help='Threads that appears in more than or equals to value of this option will be shown(default: number of all input files)')
        parser.add_argument('--show-inactive', action="store_true", help='Show Inactive threads in output')
        parser.add_argument('--simple-match', action="store_true", help="Stacks are consisdered same if all frames' names are same")

        args = parser.parse_args()

        self.inputFiles = args.rte

        if args.output is None:
            self.output = sys.stdout
        else:
            self.output = open(args.output, mode='w')

        if args.span is None:
            self.span = len(args.rte)
        else:
            self.span = args.span

        self.showInactive = args.show_inactive
        self.simpleMatch = args.simple_match


def main():
    conf = __Conf()

    lstlstThreads = []
    
    """
    Create a list of threads list from rte files
    Position within the list is important because later on the index position is used to find out the filename
    """
    for filename in conf.inputFiles:
        with open(filename) as rte:
            threads, dedup = dedupStackShort(rte)
            lstlstThreads.append(threads)

    dictThreadToFiles = findSameThread(lstlstThreads, simpleMatch = conf.simpleMatch);

    for thread, inputNums in dictThreadToFiles.items():

        if len(inputNums) < conf.span:
            continue

        if len(thread[1]) == 0 and not conf.showInactive:
            continue

        print("Files:")
        for inputNum in inputNums:
            print("[{0}]{1}".format(inputNum,conf.inputFiles[inputNum]), file=conf.output)

        print(thread[0], file=conf.output)

        if conf.simpleMatch:
            for frameNumber, frame in enumerate(thread[1], start=1):
                print(" {0}: {1}".format(frameNumber, frame), file=conf.output)
            print(file=conf.output)
        else:
            print(thread[1], file=conf.output)


if __name__ == "__main__":
	main()
