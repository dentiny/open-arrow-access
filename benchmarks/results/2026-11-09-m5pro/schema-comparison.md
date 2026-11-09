| Workload (backend/case/chunk/batch/concurrency/I/O chunk/projection/iterations/instrumented) | n | Before ms | After ms | Speedup (bootstrap 95% interval) | Before/after peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| fs/atif/65536/8192/1/2097152/all/3/False | 7/7 | 378.796 | 229.093 | 1.65× (1.64–1.66) | 35.5/35.4 |
| http/atif/65536/8192/1/2097152/all/3/False | 7/7 | 390.290 | 239.808 | 1.63× (1.58–1.64) | 40.9/40.9 |
| memory/atif/65536/8192/1/2097152/all/3/False | 7/7 | 377.179 | 229.370 | 1.64× (1.58–1.67) | 49.1/49.0 |
| raw/atif/65536/8192/1/2097152/all/3/False | 7/7 | 377.257 | 229.531 | 1.64× (1.63–1.68) | 48.5/48.5 |
