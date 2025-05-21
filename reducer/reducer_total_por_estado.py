#!/usr/bin/env python3
import sys

estado_atual = None
soma         = 0.0

for linha in sys.stdin:
    uf, v_str = linha.strip().split('\t', 1)
    try:
        v = float(v_str)
    except ValueError:
        continue

    if estado_atual is None:
        estado_atual = uf

    if uf != estado_atual:
        print(f"{estado_atual}\t{soma:.2f}")
        estado_atual = uf
        soma = 0.0

    soma += v

if estado_atual is not None:
    print(f"{estado_atual}\t{soma:.2f}")
