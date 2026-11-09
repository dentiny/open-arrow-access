| Workload (backend/case/chunk/batch/concurrency/I/O chunk/projection/iterations/instrumented) | n | Before ms | After ms | Speedup (bootstrap 95% interval) | Before/after peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| raw/atif/1024/256/1/2097152/all/8/False | 9/9 | 68.868 | 59.053 | 1.17× (1.12–1.19) | 54.1/52.3 |
| raw/atif/65536/256/1/2097152/all/8/False | 9/9 | 63.570 | 55.514 | 1.15× (1.11–1.18) | 52.2/52.3 |
| raw/lines/1024/256/1/2097152/all/8/False | 9/9 | 12.138 | 3.540 | 3.43× (3.35–3.48) | 40.1/40.2 |
| raw/lines/65536/256/1/2097152/all/8/False | 9/9 | 8.432 | 0.656 | 12.86× (12.47–13.61) | 39.7/39.8 |
