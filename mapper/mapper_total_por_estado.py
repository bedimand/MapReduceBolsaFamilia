#!/usr/bin/env python3
import sys

ANO_FIXO = "2015"

for linha in sys.stdin:
    linha = linha.strip()
    if not linha or linha.startswith("MES COMPETENCIA"):
        continue
    parts = linha.split(';')
    periodo = parts[0]       # YYYYMM
    uf      = parts[2]
    raw     = parts[8]
    valor   = raw.replace('.', '').replace(',', '.')
    try:
        v = float(valor)
    except ValueError:
        continue

    if periodo[:4] == ANO_FIXO:
        print(f"{uf}\t{v:.2f}")
