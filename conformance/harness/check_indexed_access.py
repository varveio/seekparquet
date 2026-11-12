#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Run deterministic access regressions and print a JSON report.

This repository regression runner is not a normative format qualification
protocol. It requires the declared toolchain for each selected reader.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

GO_TESTS = (
    "TestIndexedAccessColdWarmLongPrefix",
    "TestIndexedAccessRepeatedListingResume",
    "TestIndexedAccessNonMonotoneScaling",
    "TestLargeGroupResumeIsPageBounded",
    "TestSparseListingDecodesValuePageOnce",
    "TestSparseProjectionSkipsPages",
    "TestFirstKeysShareIndexBudget",
)
JAVA_CLASSES = ("SeekCostTest", "IndexedAccessTest", "IndexAndEncodingTest")


def run(command, cwd, env, timeout):
    started = time.monotonic()
    try:
        process = subprocess.Popen(
            command, cwd=cwd, env=env, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, start_new_session=True)
    except OSError as error:
        return {"command": command, "exit_code": 127, "timed_out": False,
                "seconds": 0}, str(error)
    timed_out = False
    try:
        output, _ = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        output, _ = process.communicate()
    return {"command": command, "exit_code": process.returncode,
            "timed_out": timed_out,
            "seconds": round(time.monotonic() - started, 3)}, output


def metrics(output):
    return [json.loads(line.split("ACCESS_METRIC ", 1)[1])
            for line in output.splitlines() if "ACCESS_METRIC " in line]


def run_go(root, env, timeout):
    pattern = "^(" + "|".join(GO_TESTS) + ")$"
    result, log = run(
        ["go", "test", "-json", "-count=1", "-p", "1", "-timeout", "10m",
         "-run", pattern, "."], root / "go", env, timeout)
    result.update(tests=[], measurements=[])
    for line in log.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        for metric in metrics(event.get("Output", "")):
            result["measurements"].append(dict(metric, test=event.get("Test")))
        if event.get("Action") in ("pass", "fail", "skip") and "Test" in event:
            result["tests"].append({"name": event["Test"], "status": event["Action"]})
    names = {test["name"] for test in result["tests"]}
    result["missing_tests"] = sorted(set(GO_TESTS) - names)
    return finish(result, log)


def run_java(root, env, timeout):
    # Delete only this runner's generated reports before execution. Gradle must
    # produce all selected reports afresh; stale success cannot fill a gap.
    for name in JAVA_CLASSES:
        path = (root / "java/seekparquet/build/test-results/test"
                / f"TEST-io.varve.seekparquet.{name}.xml")
        path.unlink(missing_ok=True)
    command = [str(root / "java/gradlew"), "--no-daemon", "--max-workers=2",
               ":seekparquet:test", "--rerun-tasks"]
    for name in JAVA_CLASSES:
        command.extend(["--tests", "*" + name])
    result, log = run(command, root / "java", env, timeout)
    result.update(tests=[], measurements=[], missing_tests=list(JAVA_CLASSES))
    # A failed invocation must never inherit a previous run's successful XML.
    if result["exit_code"] == 0:
        for name in JAVA_CLASSES:
            path = (root / "java/seekparquet/build/test-results/test"
                    / f"TEST-io.varve.seekparquet.{name}.xml")
            if not path.exists():
                continue
            suite = ET.parse(path).getroot()
            cases = suite.findall("testcase")
            if cases:
                result["missing_tests"].remove(name)
            for output in suite.findall("system-out"):
                for metric in metrics(output.text or ""):
                    result["measurements"].append(dict(metric, test_class=name))
            for case in cases:
                if case.find("failure") is not None or case.find("error") is not None:
                    status = "fail"
                elif case.find("skipped") is not None:
                    status = "skip"
                else:
                    status = "pass"
                result["tests"].append({"class": name, "name": case.attrib["name"],
                                        "status": status})
    return finish(result, log)


def finish(result, log):
    result["passed"] = (
        result["exit_code"] == 0 and not result.get("timed_out", False)
        and bool(result["tests"])
        and not result["missing_tests"]
        and all(test["status"] == "pass" for test in result["tests"]))
    if not result["passed"]:
        print(log[-20000:], file=sys.stderr)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--language", "--reader", choices=("both", "go", "java"), default="both")
    parser.add_argument("--timeout", type=int, default=900, help="seconds per reader")
    parser.add_argument("--output", type=Path, help="also save the JSON report")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    root = args.root.resolve()
    env = dict(os.environ, GOMAXPROCS="2")
    readers = {}
    for name, runner in (("go", run_go), ("java", run_java)):
        if args.language in ("both", name):
            readers[name] = runner(root, env, args.timeout)
    report = {"report": "indexed-access-regressions",
              "passed": all(result["passed"] for result in readers.values()),
              "freeze_qualified": False, "coverage": "docs/dev/indexed-access.md",
              "readers": readers}
    rendered = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered)
    print(rendered, end="")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
