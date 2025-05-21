#!/usr/bin/env python3
import sys

current_period = None
nis_set        = set()

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    period, nis = line.split('\t', 1)

    if current_period is None:
        current_period = period

    # quando muda de grupo, emite e reseta
    if period != current_period:
        print(f"{current_period}\t{len(nis_set)}")
        nis_set.clear()
        current_period = period

    nis_set.add(nis)

# emite o último período
if current_period is not None:
    print(f"{current_period}\t{len(nis_set)}")
