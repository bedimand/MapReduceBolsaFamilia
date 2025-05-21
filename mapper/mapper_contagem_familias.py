#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split(';')
    # pula o header
    if parts[0] == 'MES COMPETENCIA':
        continue

    # coluna 0 = mês de competência (YYYYMM), coluna 6 = NIS
    period = parts[0]
    nis    = parts[6]

    if not nis:
        continue

    # emite “chave\tvalor”
    print(f"{period}\t{nis}")
