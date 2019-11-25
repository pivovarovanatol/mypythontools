#!/usr/bin/env python3
# vim: tabstop=4 shiftwidth=4
import datetime
import sys
import re


def HANATimestampToDatetime(t):
    """
    Converts HANA Timestamp value into datetime type
    e.g.) 1521476619351496 -> 2018-03-20 01:23:39.351496
    """
    t = t.strip()
    try:
        # extrace epoch from HANA timestamp
        epoch = int(t[0:-6]) if t[0:-6] else 0
        # extrace microseconds from HANA timestamp
        micro = int(t[-6:])
    except ValueError as e:
        print(e)
        return None

    dt = datetime.datetime.fromtimestamp(epoch, tz=datetime.timezone.utc)\
        .replace(microsecond=micro)

    return dt, epoch, micro


def replacer(matchobj):
    """
    regex replacer function
    """
    dt, epoch, micro = HANATimestampToDatetime(matchobj.group(1))
    if epoch == 0 and micro == 0:
        return matchobj.group(0)
    else:
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f utc')


def main():
    for line in sys.stdin:
        print(re.sub(r'([0-9]+) \[local\]', replacer, line), end='')


if __name__ == "__main__":
    main()
