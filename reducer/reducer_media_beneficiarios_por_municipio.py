#!/usr/bin/env python3
import sys

mun_atual = None
nis_set   = set()
contagens = []

for linha in sys.stdin:
    mun, nis = linha.strip().split('\t', 1)
    if mun_atual is None:
        mun_atual = mun

    if mun != mun_atual:
        contagens.append(len(nis_set))
        nis_set.clear()
        mun_atual = mun

    nis_set.add(nis)

# último município
if mun_atual is not None:
    contagens.append(len(nis_set))

if contagens:
    media = sum(contagens) / len(contagens)
else:
    media = 0.0

print(f"MEDIA_BENEFICIARIOS\t{media:.2f}")
