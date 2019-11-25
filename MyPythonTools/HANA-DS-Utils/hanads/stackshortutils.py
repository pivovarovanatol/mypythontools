#!/usr/bin/env python3
# vim: tabstop=4 shiftwidth=4
import re

# 202656343[thr=108664]: JobWrk5176 at
# OR
# 1393088[thr=10203]: SqlExecutor is inactive
threadRegex = r"([0-9]+)\[thr.([0-9]+)\]: (.+) (is inactive|at)"
threadRegexComp = re.compile(threadRegex)

class StackShortThreadInfo:
    def __init__(self, threadString):
        threadRegexMatch = threadRegexComp.match(threadString)

        if threadRegexMatch == None:
            print("Input string is incorrect")
            sys.exit(1)

        self.context = threadRegexMatch.group(1)
        self.tid = threadRegexMatch.group(2)
        self.name = threadRegexMatch.group(3)
        self.active = threadRegexMatch.group(4)

    def __str__(self):
        return "{0}[thr={1}]: {2} {3}".format(self.context, self.tid, self.name, self.active)

    def __eq__(self, other):
        return (self.context == other.context) and (self.tid == other.tid) and (self.name == other.name) and (self.active == other.active)

    def __hash__(self):
        return hash(self.context + self.tid + self.name + self.active)


#  1: 0x00007fd226e07659 in syscall+0x15 (libc.so.6)
# OR
#  1: 0x00007fd255fcd6ba in TRexUtils::Parallel::__parallelFor::ForJob<ltt::vector_iterator<SccWrapper<IndexHash<TrexTypes::IntAttributeValue>::PartResult> >, AttributeEngine::ValueArrayColumnDict<TrexTypes::IntAttributeValue>::Row2ValueIdFor>::runEx()+0x86 at ValueArrayColumn.cpp:56 (libhdbcs.so)
frameRegex = r" *([0-9]+): (0x[0-9a-f]+) in (.*)\+(0x[0-9a-f]+)( at (.*):([0-9]+))? \((.*)\)"
frameRegexComp = re.compile(frameRegex)

class StackShortFrame:
    def __init__(self, frameString):
        frameMatch = frameRegexComp.match(frameString)
        
        self.frame = frameMatch.group(1)
        self.address = frameMatch.group(2)
        self.function = frameMatch.group(3)
        self.offset = frameMatch.group(4)
        self.source = frameMatch.group(6)
        self.line = frameMatch.group(7)
        self.lib = frameMatch.group(8)

    def __str__(self):
        if self.source is None:
            str = "{0:>2}: {1} in {2}+{3} ({6})".format(self.frame, self.address, self.function, self.offset, self.source, self.line, self.lib)
        else: 
            str = "{0:>2}: {1} in {2}+{3} at {4}:{5} ({6})".format(self.frame, self.address, self.function, self.offset, self.source, self.line, self.lib)
        
        return str


class StackShortStack(list):
    def __str__(self):
        str = ""
        for i in self:
            str = str + i.__str__() + "\n"
    
        return str

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(self.__str__())

    def dedupKey(self):
        tmpList = []

        for frame in self:
            tmpList.append(frame.function)

        return tuple(tmpList)
        

class StackShortBuilder():
    """
    A finite state machine that take [STACK_SHORT] section as input and generates two dictionaries
    1. threadDictionary : ThreadInfo --> StackShortStack
    1. dedupDictionary : StackShortStack.dedupKey() -> [ThreadsInfo*]
    """
    def __init__(self):
        self.state = "INIT"

        self.threadDictionary = {}
        self.dedupDictionary = {}

        self.workingThreadInfo = None
        self.workingCallStack = None

    def __buildResult(self):
        self.threadDictionary[self.workingThreadInfo] = self.workingCallStack
        
        dedupKey = self.workingCallStack.dedupKey()

        if dedupKey not in self.dedupDictionary:
            self.dedupDictionary[dedupKey] = []

        self.dedupDictionary[dedupKey].append(self.workingThreadInfo)
        
    # Finite State Machine 
    def input(self, inputString):
        # INIT State
        if self.state == "INIT":
            threadMatch = threadRegexComp.match(inputString)

            if threadMatch is not None:
                self.workingThreadInfo = StackShortThreadInfo(inputString)
                self.workingCallStack = StackShortStack();
                self.state = "THREAD"

            elif inputString[0:4] == '[OK]':
                self.__buildResult()
                self.state = "DONE"

            else:
                self.state = "SKIP"
                
        # THREAD State
        elif self.state == "THREAD":
            frameMatch = frameRegexComp.match(inputString)

            if frameMatch is not None:
                self.workingCallStack.append(StackShortFrame(inputString))
                self.state = "FRAME"

            elif inputString[0:2] == '--':
                self.__buildResult()
                self.state = "INIT"

            else:
                self.state = "SKIP"
        
        # FRAME State
        elif self.state == "FRAME":
            frameMatch = frameRegexComp.match(inputString)

            if frameMatch is not None:
                self.workingCallStack.append(StackShortFrame(inputString))

            elif inputString[0:2] == '--':
                self.__buildResult()
                self.state = "INIT"

            else:
                self.state = "SKIP"

        # SKIP State
        elif self.state == "SKIP":
            if inputString[0:2] == '--':
                self.state = "INIT"
            elif inputString[0:4] == '[OK]':
                self.state = "DONE"

def findSameThread(lstlstThreads, simpleMatch = False):
    """
    lstlstThread: list of threads list returned by dedupStackShort
    """

    dictThreadToFiles = {}

    for count, lstThreads in enumerate(lstlstThreads):

        for thread, stack in lstThreads.items():

            if (simpleMatch):
                threadStack = (thread, stack.dedupKey())
            else:
                threadStack = (thread, stack)

            if threadStack not in dictThreadToFiles:
                dictThreadToFiles[threadStack] = []

            dictThreadToFiles[threadStack].append(count)

    return dictThreadToFiles


def dedupStackShort(inputStream, RTEStream=True):
    stackShortBuilder = StackShortBuilder()

    if RTEStream:
        for line in inputStream:
            if line[0:13] == '[STACK_SHORT]':
                break

    for line in inputStream:
        stackShortBuilder.input(line)

        if stackShortBuilder.state == 'DONE':
            break

    return stackShortBuilder.threadDictionary, stackShortBuilder.dedupDictionary
