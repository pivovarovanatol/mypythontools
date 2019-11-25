#!/usr/bin/env python3
# vim: tabstop=4 shiftwidth=4
import argparse
import datetime
import sys
import re
import fileinput

from hanads.stackshortutils import StackShortBuilder, dedupStackShort

class __Conf:
    def __init__(self):
        parser = argparse.ArgumentParser(description="")

        parser.add_argument('-i','--input', required=False, type=str, help='Input RTE dump file to analyze. If not given stdin will be read')
        parser.add_argument('-o','--output', required=False, type=str, help='Output to file. If not given ouput will be written to stdout')

        args = parser.parse_args()

        if args.input is None:
            self.input = sys.stdin
        else:
            self.input = fileinput.input(files=[args.input])

        if args.output is None:
            self.output = sys.stdout
        else:
            self.output = open(args.output, mode='w')


def main():
    conf = __Conf()
    
    threads, dedup = dedupStackShort(conf.input)

    print("[STACK_SHORT_DEDUP]", file=conf.output)

    for key, value in dedup.items():
        if len(key) == 0:
            continue

        print("Number of Dups: {0}".format(len(value)), file=conf.output)

        for thread in value:
            print(thread, file=conf.output)

        for frameNumber, frame in enumerate(list(key), start=1):
            print(" {0}: {1}".format(frameNumber, frame), file=conf.output)

        print('--', file=conf.output)


if __name__ == "__main__":
	main()
