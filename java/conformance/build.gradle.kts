// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// The conformance adapter: a JSON-lines program that drives the library for
// conformance/harness/run.py. It is not part of the library's API.

plugins {
    application
}

dependencies {
    implementation(project(":seekparquet"))
    // parquet-java logs through SLF4J; the adapter discards it (stdout carries responses only).
    runtimeOnly(libs.slf4j.nop)
}

application {
    applicationName = "seekparquet-conformance"
    mainClass.set("io.varve.seekparquet.conformance.Adapter")
}
