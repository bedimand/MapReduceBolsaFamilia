#!/usr/bin/env python3
import sys

from collections import defaultdict

somas = defaultdict(float)

for linha in sys.stdin:
    mun, v_str = linha.strip().split('\t', 1)
    try:
        v = float(v_str)
    except ValueError:
        continue
    somas[mun] += v

# encontra o de maior soma
if somas:
    top_mun, top_val = max(somas.items(), key=lambda kv: kv[1])
    print(f"{top_mun}\t{top_val:.2f}")
