#!/usr/bin/env python
from __future__ import print_function
from collections import defaultdict
import argparse
import bson
import glob
import json
import os.path
import pprint
import re

MAX_LENGTH = 768   # 53 byte overhead?  Maximum is 1024 / characters with utf-8 encoding ...
TRUNC_LENGTH = 256


def GetIndexKeys(basename, options):
    keys = set()
    with open(basename + ".metadata.json", "r") as f:
        metadata = json.load(f)
        if options.verbose > 1:
            pprint.pprint(metadata)
        for field in metadata.get("indexes", []):
            for k in field["key"]:
                keys.add(k)

    if options.verbose:
        print("keys:", keys)

    return keys


def SafeTooLarge(s):
    try:
        return len(s.encode("utf-8", "replace")) > MAX_LENGTH
    except UnicodeDecodeError:
        print("Problem checking length:", repr(s))
        return True


def FixEntry(entry, keys, stats, options):
    fixed = False
    for k, v in entry.items():
        if k in keys and isinstance(v, basestring):
            if SafeTooLarge(v):
                #pprint.pprint(entry)
                # import pdb; pdb.set_trace()
                if options.truncate:
                    entry[k] = v[:TRUNC_LENGTH]
                    if options.verbose > 1:
                        print("Truncate:", repr(entry[k]))
                stats[k] += 1
                fixed = True
    return entry, fixed


def Process(keys, reading, writing, options):
    stats = defaultdict(int)
    count = 0
    unique = set()
    for entry in bson.decode_file_iter(reading):
        count += 1
        new_entry, fixed = FixEntry(entry, keys, stats, options)
        uid = entry.get("_id", None)
        if writing:
            if (uid is None) or (uid not in unique):
                unique.add(uid)
                if not fixed or options.truncate:
                    writing.write(bson.BSON.encode(new_entry))
            else:
                print("Skipping duplicate id:", uid)

    if options.verbose:
        pprint.pprint(stats)
    print("processed {} records".format(count))


def FixFile(filename, options):
    basename = re.match("(.*)\\.bson", filename).group(1)
    print("name:", basename)
    keys = GetIndexKeys(basename, options)
    in_name = basename + ".bson"
    out_name = basename + ".bson.renamed"
    temp_name = basename + ".bson.output"
    if os.path.exists(out_name) or os.path.exists(temp_name):
        print("Skipping - output file already exists")
    else:
        reading = None
        writing = None
        try:
            reading = open(in_name, "rb")
            if not options.dry_run:
                writing = open(temp_name, "wb")
            Process(keys, reading, writing, options)

        finally:
            if reading:
                reading.close()
            if writing:
                writing.close()
        if writing:
            os.rename(in_name, out_name)
            os.rename(temp_name, in_name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="Increase output verbosity")
    parser.add_argument("-a", "--all", action="store_true",
                        help="Fix all files")
    parser.add_argument("--truncate", action="store_true",
                        help="Attempt to truncate instead of removing too large items")
    parser.add_argument("--dry-run", action="store_true",
                        help="Not write any files - just check them")
    parser.add_argument("filename", nargs="*",
                        help="Filename to fix")
    args = parser.parse_args()

    if args.all:
        for matching in glob.glob("*.bson"):
            FixFile(matching, args)
    else:
        if len(args.filename) == 0:
            parser.error("Specify a filename or --all to fix something")
        for name in args.filename:
            FixFile(name, args)


if __name__ == "__main__":
    main()
