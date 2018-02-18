#!/usr/bin/env python
from __future__ import print_function
from builtins import str
from collections import defaultdict
import argparse
import bson
import glob
import json
import os.path
import pprint
import re

MAX_LENGTH = 768   # Maximum is 1024 - 53 byte overhead (at least).


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
        print("  Index names:", ", ".join(sorted(keys)))

    return keys


def SafeTooLarge(s):
    try:
        return len(s.encode("utf-8", "replace")) > MAX_LENGTH
    except UnicodeDecodeError:
        print("Problem checking length:", repr(s))
        return True


def InvalidEntry(entry, keys, stats):
    for k, v in entry.items():
        if k in keys and isinstance(v, str):
            if SafeTooLarge(v):
                stats[k] += 1
                return True
    return False


def Process(keys, reading, good, bad):
    stats = defaultdict(int)
    count = 0
    unique = set()
    for entry in bson.decode_file_iter(reading):
        count += 1
        invalid = InvalidEntry(entry, keys, stats)
        uid = entry.get("_id", None)
        if good and bad:
            if uid is not None:
                if uid in unique:
                    stats["_id(duplicate)"] += 1
                    invalid = True
                else:
                    unique.add(uid)

            if invalid:
                bad.write(bson.BSON.encode(entry))
            else:
                good.write(bson.BSON.encode(entry))

    for k in sorted(stats.keys()):
        print("    {}: {} violations".format(k, stats[k]))
    print("  Total {} records processed".format(count))


def FixFile(filename, options):
    basename = re.match("(.*)\\.bson", filename).group(1)
    print("Processing:", basename)
    keys = GetIndexKeys(basename, options)
    in_name = basename + ".bson"
    temp_name = basename + ".bson.temp"
    bad_name = basename + ".bson.bad"
    original_name = basename + ".bson.original"
    if os.path.exists(temp_name) or os.path.exists(bad_name) or os.path.exists(original_name):
        print("Skipping - found existing output files(.temp, .bad, .original)")
    else:
        reading = None
        good = None
        bad = None
        try:
            reading = open(in_name, "rb")
            if not options.dry_run:
                good = open(temp_name, "wb")
                bad = open(bad_name, "wb")
            Process(keys, reading, good, bad)

        finally:
            if reading:
                reading.close()
            if good:
                good.close()
            if bad:
                bad.close()
        if not options.dry_run:
            os.rename(in_name, original_name)
            os.rename(temp_name, in_name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="Increase output verbosity")
    parser.add_argument("-a", "--all", action="store_true",
                        help="Fix all files")
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
