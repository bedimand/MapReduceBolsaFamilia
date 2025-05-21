#!/usr/bin/env python3
import sys

REGIONS = {
  'AC':'Norte','AL':'Nordeste','AP':'Norte','AM':'Norte','BA':'Nordeste',
  'CE':'Nordeste','DF':'Centro-Oeste','ES':'Sudeste','GO':'Centro-Oeste',
  'MA':'Nordeste','MT':'Centro-Oeste','MS':'Centro-Oeste','MG':'Sudeste',
  'PA':'Norte','PB':'Nordeste','PR':'Sul','PE':'Nordeste','PI':'Nordeste',
  'RJ':'Sudeste','RN':'Nordeste','RS':'Sul','RO':'Norte','RR':'Norte',
  'SC':'Sul','SP':'Sudeste','SE':'Nordeste','TO':'Norte'
}

for linha in sys.stdin:
    linha = linha.strip()
    if not linha or linha.startswith("MES COMPETENCIA"):
        continue
    parts = linha.split(';')
    uf    = parts[2]
    nis   = parts[6]
    raw   = parts[8]
    region = REGIONS.get(uf)
    if region and nis:
        valor = raw.replace('.', '').replace(',', '.')
        try:
            v = float(valor)
        except ValueError:
            continue
        print(f"{region}\t{nis},{v:.2f}")
