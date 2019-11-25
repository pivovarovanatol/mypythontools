#!/usr/bin/env python3
# vim: tabstop=4 shiftwidth=4
import re
import sys
import os
import heapq
from pathlib import Path


def findTraceFiles(path):
    p = Path(path)

    reTraceFile = re.compile(r'\.(3[0-9]{4})\.([0-9]{3})\.trc$')

    for i in p.glob('**/*trc'):
        if reTraceFile.search(i.name) is not None:
            yield i


def SrvinfoFromTrcpath(filePath, fqdn=False):
    fileName = filePath.name
    indexUnderscore = fileName.index('_')

    if fqdn:
        hostname = filePath.parts[1]
    else:
        hostname = filePath.parts[1].split('.')[0]

    servicename = fileName[0:indexUnderscore]
    serviceport = fileName[-13:-8]

    return hostname, "{0}:{1}".format(servicename, serviceport)


def buildTraceListPerService(fulldumproot):
    dictServiceTrace = {}
    # setServices = set()

    for p in fulldumproot:
        for i in findTraceFiles(p):
            srvInfo = SrvinfoFromTrcpath(i.relative_to(p))

            # setServices.add(srvInfo)

            if srvInfo not in dictServiceTrace:
                dictServiceTrace[srvInfo] = []

            dictServiceTrace[srvInfo].append(i.__str__())

    # return dictServiceTrace, setServices
    return dictServiceTrace


def applyFilter(dictServiceTrace, includeHosts=None, includeServices=None):
    delKeyList = set()

    for key in dictServiceTrace.keys():

        if includeHosts is not None and key[0] not in includeHosts:
            delKeyList.add(key)

        if includeServices is not None and key[1] not in includeServices:
            delKeyList.add(key)

    for key in delKeyList:
        del dictServiceTrace[key]


class FileSeq():
    def __init__(self, files):
        self.__files = files

        self.__curIdx = 0
        self.__curFile = open(self.__files[self.__curIdx], errors='replace')
        self.__curLine = 0

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            try:
                for line in self.__curFile:
                    self.__curLine += 1
                    return line
            except UnicodeDecodeError:
                print(self.__curLine, file=sys.stderr)
                print(self.__curFile.__str__(), file=sys.stderr)
                raise

            self.__curFile.close()

            self.__curIdx += 1

            if self.__curIdx < len(self.__files):
                self.__curFile = open(self.__files[self.__curIdx],
                                      errors='replace')
                self.__curLine = 0
            else:
                break

        raise StopIteration


# [25279]{-1}[-1/-1] 2018-02-24 19:23:53.952603 i Basis            |
reHeader = re.compile(r'\[[0-9]+\]{-?[0-9]+}\[-?[0-9]+/-?[0-9]+\]')


class TraceTokenizer():

    def __init__(self, trace):
        """
        inputFiles = list of Path objects
        """
        self.__traceInput = trace
        self.__nexttimestamp = None
        self.__nextfirstline = None

        # skip trace entry without trace header line
        # (continued from previous sequence)
        self.__skipGarbage()

    def __skipGarbage(self):
        for line in self.__traceInput:
            if reHeader.match(line):
                self.__prepareNext(line)
                break

    def __iter__(self):
        return self

    def nextTimestamp(self):
        return self.__nexttimestamp

    def __prepareNext(self, entryline):

        timestampIndex = entryline.index(' ')
        timestampStr = entryline[timestampIndex+1:timestampIndex+28]

        self.__nexttimestamp = timestampStr
        self.__nextfirstline = entryline

    def __next__(self):
        if self.__nexttimestamp is None:
            raise StopIteration

        timestamp = self.__nexttimestamp
        entry = [self.__nextfirstline]

        for line in self.__traceInput:
            if reHeader.match(line):
                self.__prepareNext(line)
                return timestamp, entry
            else:
                entry.append(line)

        self.__nexttimestamp = None
        self.__nextfirstline = None

        return timestamp, entry


def mergeTrace(queueTokenizers):
    while len(queueTokenizers) > 0:
        timestamp, key, tokenizer = heapq.heappop(queueTokenizers)

        joinString = "[{0}_{1}] ".format(key[0], key[1][0:2]+key[1][-2:])

        if queueTokenizers:
            nexttimestamp = queueTokenizers[0][0]

        while True:
            yield "{0}{1}".format(joinString,
                                  joinString.join(tokenizer.__next__()[1]))

            if tokenizer.nextTimestamp() is None:
                # The very last line of the trace doesn't have the line break.
                # Explicitely adding a line break to keep the formatting
                yield os.linesep
                break

            if tokenizer.nextTimestamp() > nexttimestamp:
                heapq.heappush(queueTokenizers,
                               (tokenizer.nextTimestamp(), key, tokenizer))
                break
