#!/usr/bin/env python3
from __future__ import print_function
import select
import os
import argparse
import fnmatch
import localstreamredis
import fileinput
import sys
import datetime

redisHost = os.getenv("REDISHOST")
redisPassword = os.getenv("REDISPASSWORD", None)

def eprint(whatever):
    print(whatever, file=sys.stderr)

r = localstreamredis.StrictRedis(redisHost,password=redisPassword)

class FullErrorParser(argparse.ArgumentParser):
   def error(self, message):
      sys.stderr.write('error: %s\n' % message)
      self.print_help()
      sys.exit(2)

def getAllStreams(r, keys=None):
    keyList = r.keys() if keys is None else keys
    pipe = r.pipeline()
    for key in keyList: pipe.type(key)
    return [x[0].decode() for x in zip(keyList,pipe.execute()) if x[1]==b'stream']

def getStreamsToMonitor(args):
    specificStreams = set()
    wildcardStreams = []
    for stream in args.streams:
        if "*" in stream or "?" in stream:
            wildcardStreams.append(stream)
        else:
            specificStreams.add(stream)
    if len(wildcardStreams):
        allStreams = getAllStreams(r)
        for thisWildCard in wildcardStreams:
            for thisStream in allStreams:
                if fnmatch.fnmatch(thisStream, thisWildCard):
                    specificStreams.add(thisStream)
    if not len(specificStreams):
        raise Exception("No Streams found to monitor.")
    eprint("Monitoring following streams:")
    for thisSpecificStream in specificStreams:
        eprint(thisSpecificStream)
    return list(specificStreams)

def from_stdin(args):
    if len(args.streams) > 1 or "*" in args.streams[0] or "?" in args.streams[0]:
        raise Exception("When processing stdin, only one stream argument (without wildcards) is required.")
    targetStream = args.streams[0]
    targetKey = args.key
    for line in fileinput.input(args.file):
        r.xadd(targetStream, maxlen=args.maxlen, **dict({targetKey: line.rstrip()}))
    pass

def to_stdout(args):
    streamsToMonitor = getStreamsToMonitor(args)
    startIndex = 0 if args.all else "$"
    currentStreamDict = dict([(x, "$") for x in streamsToMonitor])
    redis_stream = r.streams(currentStreamDict, stop_on_timeout=False, count=500)
    formatString = ""
    if args.timestamp:
        formatString = formatString + "{timestamp}: "
    if args.showstream:
        max_stream_name_len = max([len(y) for y in streamsToMonitor])
        formatString = formatString + "{{streamname:{maxlen}.{maxlen}}}: ".format(maxlen=max_stream_name_len)
    if args.index:
        formatString = formatString + "{index}: "
    if args.keyout:
        formatString = formatString + "{keyout}: "

    formatString = formatString + "{value}"

    for msg in redis_stream:
       try:
        if msg is not None:
            for key, val in msg[2].items():
                timestamp = datetime.datetime.fromtimestamp(int(msg[1][:13])/1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if args.timestamp else None
                index = msg[1].decode() if isinstance(msg[1],bytes) else msg[1]
                print(formatString.format(**dict(timestamp=timestamp,
                                                 streamname=msg[0],
                                                 index=index,
                                                 keyout=key,
                                                 value=val.decode() if isinstance(val,bytes) else val)))

       except Exception as e:
           print(e)



def stredis():
    parser = FullErrorParser(description="Treats Redis Streams like stdin or stdout.", add_help=False)
    parser.add_argument('--help', action='help', help='Show this help message and exit')
    parser.add_argument('streams', nargs='+',
                        help='Streams to be followed (or a single stream to which stdin is funneled).')
    parser.add_argument(
        '--hostname', '-h', default=os.getenv("REDISHOST", "localhost"),
        help="Hostname or IP address of Redis Server (default: localhost).")
    parser.add_argument(
        '--port', '-p', default=os.getenv("REDISPORT", 6379), type=int,
        help="Port number of Redis Server (default: 6379).")
    parser.add_argument(
        '--auth', '-a', default=os.getenv("REDISPASSWORD", None),
        help="Password of Redis Server (default: None).")
    parser.add_argument(
        '--key', '-k', default="default", help="The key association with inbound data (default 'default') (stdin only)")
    parser.add_argument(
        '--maxlen', '-m', type=int, default=None, help="Maximum length of queue (default None) (stdin only)")
    parser.add_argument(
        '--showstream', '-s', action='store_true',
        help="Show the stream associated with an outbound message.")
    parser.add_argument(
        '--index', '-i', action='store_true',
        help="Show indexes for outbound data.")
    parser.add_argument(
        '--timestamp', '-t', action='store_true',
        help="Show ISO decoded timestamp for message index.")
    parser.add_argument(
        '--keyout', '-ko', action='store_true',
        help="Show the key for an outbound message.")
    parser.add_argument(
        '--file', '-f', default="-",
        help="Grabs input from a file rather than stdin.")
    parser.add_argument(
        '--all', action='store_true',
        help="Shows everything in the stream history too.")


    args = parser.parse_args()

    if select.select([sys.stdin,],[],[],0.0)[0] or args.file != "-":
        from_stdin(args)
    else:
        to_stdout(args)

if __name__== "__main__":
    stredis()