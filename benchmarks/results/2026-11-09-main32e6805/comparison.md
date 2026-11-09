# Independent optimization comparisons

Time is milliseconds per complete scan. Native comparisons alternate ten isolated processes per variant. Adapter comparisons use five independently isolated processes per variant, with normalized iterations. RSS includes setup, input fixtures and allocator retention.

| Change | Before ms | After ms | Median speedup | Bootstrap 95% interval | Before/after process peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| named-first | 1.595747 | 0.000076 | 20980.675× | 19588.624–21986.415× | 34.53/24.52 |
| named-list | 1.850843 | 1.813997 | 1.020× | 0.966–1.123× | 34.53/24.50 |
| atif | 37.252426 | 26.821998 | 1.389× | 1.309–1.502× | 18.92/17.97 |
| atif-projected | 15.907718 | 11.884671 | 1.339× | 1.250–1.407× | 14.98/15.09 |
| atif batch=1 | 81.567964 | 67.632219 | 1.206× | 1.187–1.236× | 15.14/15.19 |
| atif-projected batch=1 | 13.689995 | 12.960095 | 1.056× | 1.041–1.070× | 14.84/14.89 |
| atif native | 206.872889 | 184.347285 | 1.122× | 1.107–1.127× | 8.90/9.05 |
| xml native | 19.774150 | 17.292111 | 1.144× | 1.123–1.170× | 7.06/7.05 |
| warc native | 16.204756 | 14.850923 | 1.091× | 1.079–1.107× | 11.33/11.34 |

The full named-list timing interval includes 1×: no full-scan throughput improvement is claimed. The first-item result removes work proportional to the 100,000-name manifest; its very large ratio is specific to early cancellation of that in-memory workload. Native conversion results are not adapter end-to-end claims. These changes are independent and their speedups must not be multiplied.
