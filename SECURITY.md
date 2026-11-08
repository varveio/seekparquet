# Security policy

## Reporting a vulnerability

Please report suspected vulnerabilities **privately**, not in a public issue.

Use GitHub's [private vulnerability reporting][gh-report] on this repository (the **Security** tab, then
**Report a vulnerability**). If that is unavailable to you, email **oss@varve.io**.

Include enough to reproduce: the affected component and version or commit, a dataset or the steps that
produce one, and the observed and expected behaviour. We aim to acknowledge a report within five business
days and will keep you informed while we investigate. Please do not disclose the issue publicly until a fix is
released and a disclosure date agreed.

[gh-report]: https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability

## Supported versions

Every component is pre-1.0. Security fixes go to `main` and the latest release of each component.

## Scope

SeekParquet readers open files that may come from untrusted sources. In scope, for example:

- a dataset that makes a reader read outside its dataset root (path confinement, spec PART-1);
- a dataset that makes a reader allocate memory or do work far beyond what the files' declared sizes justify;
- a crash, hang or panic on malformed input;
- a dataset that a reader accepts and answers wrongly instead of refusing.

A reader is not expected to detect corruption in data it never decodes; see the specification, section 7.
