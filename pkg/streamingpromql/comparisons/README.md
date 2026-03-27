This directory contains tests that evaluate test cases using both MQE and the Prometheus engine to verify they produce the same results.
It is its own package so that it can be run without the race detector which results in significant slow-downs.
