| Workload (backend/case/chunk/batch/concurrency/I/O chunk/projection/iterations/instrumented) | n | Before ms | After ms | Speedup (bootstrap 95% interval) | Before/after peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| raw/atif/1024/256/1/2097152/all/8/False | 9/9 | 68.868 | 66.502 | 1.04× (1.01–1.07) | 54.1/52.2 |
| raw/atif/65536/256/1/2097152/all/8/False | 9/9 | 63.570 | 62.457 | 1.02× (1.01–1.05) | 52.2/52.2 |
| raw/lines/1024/256/1/2097152/all/8/False | 9/9 | 12.138 | 11.746 | 1.03× (1.00–1.04) | 40.1/40.1 |
| raw/lines/65536/256/1/2097152/all/8/False | 9/9 | 8.432 | 8.276 | 1.02× (1.00–1.06) | 39.7/39.7 |
