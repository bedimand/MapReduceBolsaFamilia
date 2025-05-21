#!/usr/bin/env python3
import sys
from collections import defaultdict

# região → { nis → soma_valores }
dados = defaultdict(lambda: defaultdict(float))

for linha in sys.stdin:
    reg, resto = linha.strip().split('\t', 1)
    nis, v_str = resto.split(',', 1)
    try:
        v = float(v_str)
    except ValueError:
        continue
    dados[reg][nis] += v

for reg, fam_map in dados.items():
    total = sum(fam_map.values())
    n     = len(fam_map)
    media = total / n if n else 0.0
    print(f"{reg}\t{media:.2f}")
