// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// The SeekParquet reader library, published as io.varve:seekparquet.

plugins {
    `java-library`
    `maven-publish`
}

base {
    archivesName.set("seekparquet")
}

java {
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    // InputFile and SeekableInputStream are part of the public storage contract.
    api(libs.parquet.common)
    // Parquet metadata (footer, ColumnIndex, OffsetIndex, page headers) and
    // value decoding come from parquet-java's Hadoop-free modules. See
    // QUALIFICATION.md for why parquet-hadoop is not used.
    implementation(libs.parquet.format.structures)
    implementation(libs.parquet.column)
    // Pure-Java SNAPPY and ZSTD decompression; GZIP uses java.util.zip.
    implementation(libs.aircompressor)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.named<Test>("test") {
    // Tests read the shared conformance fixtures from the repository.
    systemProperty("seekparquet.conformance", rootProject.projectDir.resolve("../conformance").canonicalPath)
    // -Pseekparquet.test.readAhead=n runs every listing that asks for none with read-ahead n (ReadAheadSwitch).
    providers.gradleProperty("seekparquet.test.readAhead").orNull?.let { systemProperty("seekparquet.test.readAhead", it) }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifactId = "seekparquet"
            from(components["java"])
            pom {
                name.set("SeekParquet")
                description.set("A reader for SeekParquet, a sorted-key profile for Apache Parquet.")
                licenses {
                    license {
                        name.set("Apache-2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }
            }
        }
    }
}
