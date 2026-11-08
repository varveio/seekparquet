// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Shared conventions: every project compiles for Java 21 with a Java 21
// toolchain, treats compiler warnings as errors, and tests on JUnit 5.

subprojects {
    group = "io.varve"
    version = "0.1.0-SNAPSHOT"

    apply(plugin = "java")

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.release.set(21)
        options.encoding = "UTF-8"
        options.compilerArgs.addAll(listOf("-Xlint:all,-serial,-processing,-try", "-Werror"))
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        testLogging {
            events("failed")
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        }
    }

    tasks.withType<Javadoc>().configureEach {
        (options as StandardJavadocDocletOptions).apply {
            encoding = "UTF-8"
            addBooleanOption("Xdoclint:all", true)
            addStringOption("Xwerror", "-quiet")
        }
    }
}
