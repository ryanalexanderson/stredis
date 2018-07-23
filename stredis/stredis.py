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
r = localstreamredis.StrictRedis(redisHost, password=redisPassword)


class FullErrorParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)


def eprint(whatever):
    print(whatever, file=sys.stderr)


def get_all_streams(redis_conn, keys=None):
    key_list = redis_conn.keys() if keys is None else keys
    pipe = redis_conn.pipeline()
    for key in key_list:
        pipe.type(key)
    return [x[0].decode() for x in zip(key_list, pipe.execute()) if x[1] == b'stream']


def get_streams_to_monitor(args):
    specific_streams = set()
    wildcard_streams = []
    for stream in args.streams:
        if "*" in stream or "?" in stream:
            wildcard_streams.append(stream)
        else:
            specific_streams.add(stream)
    if len(wildcard_streams):
        all_streams = get_all_streams(r)
        for thisWildCard in wildcard_streams:
            for this_stream in all_streams:
                if fnmatch.fnmatch(this_stream, thisWildCard):
                    specific_streams.add(this_stream)
    if not len(specific_streams):
        raise Exception("No Streams found to monitor.")
    eprint("Monitoring following streams:")
    for thisSpecificStream in specific_streams:
        eprint(thisSpecificStream)
    return list(specific_streams)


def from_stdin(args):
    if len(args.streams) > 1 or "*" in args.streams[0] or "?" in args.streams[0]:
        raise Exception("When processing stdin, only one stream argument (without wildcards) is required.")
    target_stream = args.streams[0]
    target_key = args.key
    for line in fileinput.input(args.file):
        r.xadd(target_stream, maxlen=args.maxlen, **dict({target_key: line.rstrip()}))
    pass


def to_stdout(args):
    streams_to_monitor = get_streams_to_monitor(args)
    start_index = 0 if args.all else "$"
    current_stream_dict = dict([(x, start_index) for x in streams_to_monitor])
    redis_stream = r.streams(current_stream_dict, stop_on_timeout=False, count=500)
    format_string = ""
    if args.timestamp:
        format_string = format_string + "{timestamp}: "
    if args.showstream:
        max_stream_name_len = max([len(y) for y in streams_to_monitor])
        format_string = format_string + "{{streamname:{maxlen}.{maxlen}}}: ".format(maxlen=max_stream_name_len)
    if args.index:
        format_string = format_string + "{index}: "
    if args.keyout:
        format_string = format_string + "{keyout}: "

    format_string = format_string + "{value}"

    for msg in redis_stream:
        try:
            if msg is not None:
                for key, val in msg[2].items():
                    timestamp = datetime.datetime.fromtimestamp(int(msg[1][:13])/1000)\
                        .strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if args.timestamp else None
                    index = msg[1].decode() if isinstance(msg[1], bytes) else msg[1]
                    print(format_string.format(**dict(timestamp=timestamp,
                                                      streamname=msg[0],
                                                      index=index,
                                                      keyout=key,
                                                      value=val.decode() if isinstance(val, bytes) else val)))

        except Exception as e:
            print(e)


def stredis():
    parser = FullErrorParser(description="Treats Redis Streams like stdin or stdout. " +
                                         "e.g. Use 'stredis --all-streams' to monitor all streams on the server.",
                                         add_help=False)
    parser.add_argument('--help', action='help', help='Show this help message and exit')
    parser.add_argument('streams', nargs='?',
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
        '--all-messages', action='store_true',
        help="Shows everything in the stream history.")
    parser.add_argument(
        '--all-streams', action='store_true',
        help="Shows everything in the stream history.")
    parser.add_argument(
        '--list', action='store_true',
        help="Lists all of the streams on the redis server and quits.")

    args = parser.parse_args()
    if args.list:
        all_streams = get_all_streams(r)
        for this_stream in all_streams:
            print(this_stream)
            exit(0)

    elif not args.streams:
        parser.print_help()
        exit(-1)

    elif select.select([sys.stdin], [], [], 0.0)[0] or args.file != "-":
        from_stdin(args)
    else:
        to_stdout(args)


if __name__ == "__main__":
    stredis()
