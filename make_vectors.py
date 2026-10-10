#!/usr/bin/env python3
"""Turn the recorded contract messages into the exact bytes both sides must agree on.

    python proto/make_vectors.py          # write vectors/*.bin from vectors/*.txtpb
    python proto/make_vectors.py --check  # fail if any .bin is missing or stale

Each vectors/NAME.txtpb is one Envelope in protobuf text format, written by a
person and commented. NAME.bin is its encoding, produced here by the reference
Python implementation with deterministic serialisation (map keys sorted).

The app's tests (Kotlin, Wire) must decode every .bin to exactly the expected
object and encode that object back to exactly the same bytes. The gateway's
tests (Python) do the same. That is the "same recorded message sets on every
commit" from docs/handoff.md section 14.

Needs grpcio-tools (for protoc) and protobuf, e.g. the fake rover's venv:
    fake-rover/.venv/Scripts/python proto/make_vectors.py --check
"""

import argparse
import glob
import os
import sys

from google.protobuf import text_format

from pyload import load_api

HERE = os.path.dirname(os.path.abspath(__file__))
VECTORS = os.path.join(HERE, "vectors")


def encode(pb, txtpb_path):
    with open(txtpb_path, encoding="utf-8") as f:
        env = text_format.Parse(f.read(), pb.Envelope())
    return env.SerializeToString(deterministic=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="verify instead of writing")
    args = parser.parse_args()

    pb = load_api()
    sources = sorted(glob.glob(os.path.join(VECTORS, "*.txtpb")))
    if not sources:
        sys.exit("no vectors found")

    problems = 0
    for src in sources:
        name = os.path.splitext(os.path.basename(src))[0]
        data = encode(pb, src)
        out = os.path.join(VECTORS, name + ".bin")
        if args.check:
            if not os.path.exists(out):
                print(f"MISSING  {name}.bin")
                problems += 1
            elif open(out, "rb").read() != data:
                print(f"STALE    {name}.bin (run make_vectors.py)")
                problems += 1
            else:
                print(f"ok       {name} ({len(data)} bytes)")
        else:
            with open(out, "wb") as f:
                f.write(data)
            print(f"wrote    {name}.bin ({len(data)} bytes)")

    stray = {os.path.splitext(p)[0] for p in glob.glob(os.path.join(VECTORS, "*.bin"))} - \
            {os.path.splitext(p)[0] for p in sources}
    for s in sorted(stray):
        print(f"STRAY    {os.path.basename(s)}.bin has no .txtpb")
        problems += 1
    if problems:
        sys.exit(1)


if __name__ == "__main__":
    main()
