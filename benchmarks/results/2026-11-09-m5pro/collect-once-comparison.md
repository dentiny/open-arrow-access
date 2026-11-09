| Workload (backend/case/chunk/batch/concurrency/I/O chunk/projection/iterations/instrumented) | n | Before ms | After ms | Speedup (bootstrap 95% interval) | Before/after peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| fs/all/65536/8192/1/2097152/all/1/False | 7/7 | 2.556 | 2.380 | 1.07× (1.04–1.09) | 32.3/28.3 |
| http/all/65536/8192/1/2097152/all/1/False | 7/7 | 9.682 | 9.703 | 1.00× (0.91–1.06) | 34.1/33.6 |
| memory/all/65536/8192/1/2097152/all/1/False | 7/7 | 1.313 | 1.258 | 1.04× (0.87–1.09) | 42.2/40.2 |
| raw/all/65536/8192/1/2097152/all/1/False | 7/7 | 1.287 | 1.205 | 1.07× (0.90–1.16) | 41.6/39.6 |
