| Workload (backend/case/chunk/batch/concurrency/I/O chunk/projection/iterations/instrumented) | n | Before ms | After ms | Speedup (bootstrap 95% interval) | Before/after peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| fs/all/65536/8192/1/2097152/all/10/False | 7/7 | 2.096 | 1.231 | 1.70× (1.63–1.75) | 178.9/30.5 |
| fs/sequential/65536/8192/1/2097152/all/10/False | 7/7 | 0.802 | 0.800 | 1.00× (0.96–1.07) | 14.4/14.4 |
| http/all/65536/8192/1/2097152/all/10/False | 7/7 | 5.659 | 4.658 | 1.21× (1.19–1.27) | 182.7/37.9 |
| http/sequential/65536/8192/1/2097152/all/10/False | 7/7 | 4.402 | 4.301 | 1.02× (0.98–1.11) | 21.6/21.8 |
| memory/all/65536/8192/1/2097152/all/10/False | 7/7 | 1.072 | 0.360 | 2.98× (2.90–3.07) | 186.8/40.3 |
| memory/sequential/65536/8192/1/2097152/all/10/False | 7/7 | 0.024 | 0.024 | 1.01× (0.97–1.05) | 24.3/24.2 |
| raw/all/65536/8192/1/2097152/all/10/False | 7/7 | 1.043 | 0.347 | 3.00× (2.96–3.15) | 186.1/39.7 |
| raw/sequential/65536/8192/1/2097152/all/10/False | 7/7 | 0.011 | 0.011 | 1.02× (0.99–1.09) | 23.6/23.6 |
