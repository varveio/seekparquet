// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// The conformance adapter: a JSON-lines program that drives the library for
// conformance/harness/run.py. It is not part of the library's API.

plugins {
    application
}

dependencies {
    implementation(project(":seekparquet"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    // parquet-java logs through SLF4J; the adapter discards it (stdout carries responses only).
    runtimeOnly(libs.slf4j.nop)
}

application {
    applicationName = "seekparquet-conformance"
    mainClass.set("io.varve.seekparquet.conformance.Adapter")
}

// Shared fixtures exercise adapter paging, including repeated-key and prefix cursors.
tasks.test {
    systemProperty("seekparquet.conformance", rootProject.projectDir.resolve("../conformance").canonicalPath)
}
