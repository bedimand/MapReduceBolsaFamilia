#!/usr/bin/env python3
import sys

UF_FIXA = "SP"

for linha in sys.stdin:
    linha = linha.strip()
    if not linha or linha.startswith("MES COMPETENCIA"):
        continue
    parts = linha.split(';')
    uf   = parts[2]
    mun  = parts[3]
    nis  = parts[6]
    if uf == UF_FIXA and nis:
        print(f"{mun}\t{nis}")
