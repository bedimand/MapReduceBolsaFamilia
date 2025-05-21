#!/usr/bin/env bash
set -euo pipefail

# caminho pro jar do streaming (ajuste se necessário)
STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar"

# base HDFS
HDFS_BASE="/user/bedimand/bolsa-familia"
INPUT_PATH="$HDFS_BASE/csv"

# pasta local de resultados
RESULTS_DIR="results"
mkdir -p "$RESULTS_DIR"

# função para rodar um job e coletar output
# args: 1=nome_job, 2=mapper, 3=reducer
run_job() {
  local name="$1"
  local mapper="$2"
  local reducer="$3"
  local out_hdfs="$HDFS_BASE/output/$name"
  local out_txt="$RESULTS_DIR/$name.txt"

  echo "==> Executando job $name …"
  # limpa saída anterior
  hdfs dfs -rm -r -f "$out_hdfs"

  # lança streaming
  hadoop jar $STREAMING_JAR \
    -D mapreduce.job.name="$name" \
    -files "$mapper","$reducer" \
    -mapper "./$(basename $mapper)" \
    -reducer "./$(basename $reducer)" \
    -input  "$INPUT_PATH" \
    -output "$out_hdfs"

  # busca resultado e grava localmente
  echo "==> Coletando resultado em $out_txt"
  hdfs dfs -cat "$out_hdfs"/part-* > "$out_txt"
  echo "   → $name OK, $out_txt gerado"
  echo
}

# 3) media_beneficiarios_por_municipio
run_job \
  media_beneficiarios_por_municipio \
  mapper/mapper_media_beneficiarios_por_municipio.py \
  reducer/reducer_media_beneficiarios_por_municipio.py


echo "Todos os jobs completos. Veja os .txt em $RESULTS_DIR/"
