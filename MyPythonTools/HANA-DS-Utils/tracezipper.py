#!/usr/bin/env python3
# vim: tabstop=4 shiftwidth=4
import sys
import argparse
import heapq
from hanads.traceutils import buildTraceListPerService, \
    TraceTokenizer, FileSeq, mergeTrace


class __Conf:
    def __init__(self):
        parser = argparse.ArgumentParser(description="Merge traces of multiple services in timestamp order")
        group1 = parser.add_argument_group("Show Services")
        group1.add_argument('-s','--show-services', action='store_true',  help='Show all available services in the fsid')

        group2 = parser.add_argument_group("Zipping")
        group2.add_argument('-o','--output', required=False, type=str, help='Output to file. If not given ouput will be written to stdout')
        group2.add_argument('-i','--include-services', type=str, help='Comma seperated list of service index')
        parser.add_argument('fsidpath', nargs='+', help='Path to full system info dump directory')

        args = parser.parse_args()

        self.fsidpath = args.fsidpath

        if args.include_services is not None:
            self.include_services = \
                set([int(x.strip()) for x in args.include_services.split(",")])
        else:
            self.include_services = None

        if args.output is None:
            self.output = sys.stdout
        else:
            self.output = open(args.output, mode='w')

        self.show_services = args.show_services


conf = __Conf()


def main():
    tracedictionary = buildTraceListPerService(conf.fsidpath)
    services = tracedictionary.keys()

    if (conf.show_services):
        for count, service in enumerate(sorted(list(services))):
            print("{0}: {1}".format(count, service))

        sys.exit(0)

    queue = []

    for count, key in enumerate(sorted(list(services))):
        if conf.include_services is None or count in conf.include_services:
            tracetokenizer = TraceTokenizer(FileSeq(tracedictionary[key]))
            heapq.heappush(
                queue, (tracetokenizer.nextTimestamp(), key, tracetokenizer))

    for entry in mergeTrace(queue):
        print(entry, end='')


if __name__ == "__main__":
    main()
