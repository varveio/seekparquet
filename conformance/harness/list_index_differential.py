#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Optional random-fixture differential, not a conformance qualification run.

Compare complete listing answers from both adapters, with and without the index.
Generate fixtures using TestListIndexExportRandomFixtures; see ../README.md.
"""

import argparse
import base64
import json
import random
import shlex
from pathlib import Path

from run import Adapter


def b64(value):
    return base64.b64encode(value).decode("ascii")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--go-adapter", required=True)
    parser.add_argument("--java-adapter", required=True)
    args = parser.parse_args()
    fixtures = sorted(args.fixtures.resolve().glob("seed-*/keys.json"))
    if not fixtures:
        parser.error("no exported random fixtures")
    adapters = []
    requests = 0
    try:
        for command in (args.go_adapter, args.java_adapter):
            for indexed in (False, True):
                env = ["env", "SEEKPARQUET_LIST_INDEX=1"] if indexed else ["env", "-u", "SEEKPARQUET_LIST_INDEX"]
                adapters.append(Adapter(env + shlex.split(command), timeout=60))

        def compare(query):
            nonlocal requests
            responses = [adapter.call(query) for adapter in adapters]
            requests += 1
            for response in responses:
                response.pop("id", None)
                if response.get("status") != "ok" or response != responses[0]:
                    raise AssertionError(f"request={query}\nresponses={responses}")
            return responses[0].get("entries", [])

        for seed, metadata in enumerate(fixtures):
            keys = [base64.b64decode(k) for k in json.loads(metadata.read_text())]
            rng = random.Random(seed)
            base = {"op": "list", "dataset": str(metadata.parent), "delimiter_base64": b64(b"/"), "columns": ["size"]}
            prefixes = [b"", b"/", b"//", b"a", b"a/", b"b/", b"\xff", b"wide/", b"wide/d00"]
            for _ in range(200):
                key = rng.choice(keys)
                prefix = rng.choice(prefixes + [key[:rng.randrange(len(key) + 1)]])
                query = dict(base, prefix_base64=b64(prefix), limit=rng.choice([1, 2, 4, 1000]))
                cursor = rng.choice(keys + [b"missing/", b"a/x", b"\xff" * 10, b""])
                kind = rng.randrange(3)
                if kind == 1:
                    query["after"] = {"prefix_base64": b64(cursor)}
                elif kind == 2:
                    query["after"] = {"key_base64": b64(cursor), "offset": rng.randrange(4)}
                if rng.randrange(2):
                    end = rng.choice(keys)
                    query["end_base64"] = b64(rng.choice([end, end + b"\0", end[:-1]]))
                compare(query)
            for prefix in prefixes:
                for limit in (1, 2, 4, 1000):
                    query = dict(base, prefix_base64=b64(prefix), limit=limit)
                    for _ in range(len(keys) + 1):
                        entries = compare(query)
                        if len(entries) < limit:
                            break
                        last = entries[-1]
                        query["after"] = ({"prefix_base64": last["key_base64"]} if last["kind"] == "prefix"
                                          else {"key_base64": last["key_base64"], "offset": last["offset"]})
                    else:
                        raise AssertionError(f"pagination did not terminate: {query}")
        print(f"PASS: {len(fixtures)} shared random fixtures, {requests} requests, Go/Java with and without list indexes")
    finally:
        for adapter in adapters:
            extra, problems = adapter.close()
            if extra or problems:
                raise AssertionError(f"adapter shutdown: {extra}, {problems}")


if __name__ == "__main__":
    main()
