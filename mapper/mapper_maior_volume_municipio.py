#!/usr/bin/env python3
import sys

PERIODO_FIXO = "201505"

for linha in sys.stdin:
    linha = linha.strip()
    if not linha or linha.startswith("MES COMPETENCIA"):
        continue
    parts   = linha.split(';')
    periodo = parts[0]
    mun_cod = parts[3]
    raw     = parts[8]
    valor   = raw.replace('.', '').replace(',', '.')
    try:
        v = float(valor)
    except ValueError:
        continue

    if periodo == PERIODO_FIXO:
        print(f"{mun_cod}\t{v:.2f}")
