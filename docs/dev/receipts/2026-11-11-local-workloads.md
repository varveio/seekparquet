# Local workload smoke receipt: 2026-11-11

Reader revision: not recorded in the source measurement. Treat this as a historical smoke receipt,
not a measurement of the current reader. Source: `e98bb77:docs/workloads.md`.
[Recipes and fixture geometry](../benchmarks.md#reproduce-local-measurements) supply the commands and workload setup.

The two commands in [Reproduce local measurements](../benchmarks.md#reproduce-local-measurements)
were run on 2026-11-11 on Linux amd64, Intel(R) Xeon(R)
Processor @ 2.10GHz (as `/proc/cpuinfo` names it), Go 1.27.4, and
`GOMAXPROCS=2`, in a shared managed workspace. No cache flush or resource
isolation was performed. The table is from the second command: each elapsed
time is the median of its three `-count` runs of three iterations each. Read
counts and requested bytes were the same in every run. These numbers
demonstrate execution and instrumentation; repeat with longer runs before
drawing latency conclusions. The 100000-part case was not run.

| Operation | 10 parts elapsed (ms) | 1000 parts elapsed (ms) | 1000 parts ReadAt calls | 1000 parts requested bytes |
| --- | ---: | ---: | ---: | ---: |
| OpenDir + close | 0.124 | 7.350 | uninstrumented | uninstrumented |
| Counted open + close | 0.089 | 7.606 | 1 | 230178 |
| Cold point | 0.126 | 0.162 | 13 | 4142 |
| Warm point | 0.018 | 0.021 | 2 | 974 |
| Cold range100 | 0.205 | 0.439 | 30 | 8795 |
| Warm range100 | 0.076 | 0.096 | 14 | 6577 |
| Cold delimiter | 0.611 | 8.375 | 700 | 236494 |
| Warm delimiter | 0.082 | 1.160 | 300 | 79094 |
| Cold late resume | 0.122 | 0.289 | 14 | 4784 |
| Warm late resume | 0.021 | 0.025 | 3 | 1616 |

At 1000 parts the open is one read of the 230178-byte manifest and opens no
part. Cold point, range and resume queries
touch one part and make the same number of reads at 10 and 1000 parts. The
delimiter listing returns one common prefix per part, so at 1000 parts it
touches 100 parts and at 10 parts all ten; at both sizes it made 7 reads per
touched part cold and 3 warm (70/30 at 10 parts, 700/300 at 1000).

The open runs occur sequentially, so their times cannot isolate instrumentation
overhead. This harness does not measure retained heap, retained footer/index
bytes, peak descriptors, remote retries, no-OffsetIndex value-header walks or
read-ahead amplification. Add those scenarios and measurements before making
production capacity or remote-cost claims. No object-store Source implementation ships here;
remote qualification remains open. See [storage Sources](../../guides/storage-sources.md) for the implementation contract
and [remote I/O](../../guides/remote-io.md) for physical-request measurement.
The benchmark does not relax [CHECK-1](../../../spec/format-1.md#check-1): every part a query
touches is checked on that first touch.

