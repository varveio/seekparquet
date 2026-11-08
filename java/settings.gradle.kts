// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

rootProject.name = "seekparquet-java"

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        // Maven Central, addressed by its canonical host.
        mavenCentral { url = uri("https://repo1.maven.org/maven2/") }
    }
}

include("seekparquet", "conformance")
