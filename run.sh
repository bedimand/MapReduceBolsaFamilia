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
# args: 1=nome_job, 2=mapper, 3=reducer, 4=descrição
run_job() {
  local name="$1"
  local mapper="$2"
  local reducer="$3"
  local description="$4"
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

  # busca resultado e grava localmente com cabeçalho explicativo
  echo "==> Coletando resultado em $out_txt"
  {
    echo "========================================================================"
    echo "JOB: $name"
    echo "========================================================================"
    echo "DESCRIÇÃO: $description"
    echo "EXECUTADO EM: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "DADOS DE ENTRADA: $INPUT_PATH"
    echo "========================================================================"
    echo ""
    echo "RESULTADOS:"
    echo "----------"
    hdfs dfs -cat "$out_hdfs"/part-*
    echo ""
    echo "========================================================================"
  } > "$out_txt"
  echo "   → $name OK, $out_txt gerado"
  echo
}

# 1) contagem_familias
run_job \
  contagem_familias \
  mapper/mapper_contagem_familias.py \
  reducer/reducer_contagem_familias.py \
  "PERGUNTA: Quantas famílias foram beneficiadas em um determinado período (por ano ou mês)? | RESPOSTA: Contagem de famílias únicas (NIS distintos) por mês de competência"

# 2) total_por_estado
run_job \
  total_por_estado \
  mapper/mapper_total_por_estado.py \
  reducer/reducer_total_por_estado.py \
  "PERGUNTA: Qual foi o valor total pago por estado em um determinado ano? | RESPOSTA: Valor total do Bolsa Família por estado (UF) para o ano de 2015"

# 3) media_beneficiarios_por_municipio
run_job \
  media_beneficiarios_por_municipio \
  mapper/mapper_media_beneficiarios_por_municipio.py \
  reducer/reducer_media_beneficiarios_por_municipio.py \
  "PERGUNTA: Qual foi a média de beneficiários por município em determinado estado? | RESPOSTA: Média de beneficiários por município APENAS para o estado de SÃO PAULO (SP)"

# 4) media_valor_por_regiao
run_job \
  media_valor_por_regiao \
  mapper/mapper_media_valor_por_regiao.py \
  reducer/reducer_media_valor_por_regiao.py \
  "PERGUNTA: Qual foi o valor médio recebido por família em cada região do país? | RESPOSTA: Valor médio recebido por família em cada região geográfica do Brasil"

# 5) maior_volume_municipio
run_job \
  maior_volume_municipio \
  mapper/mapper_maior_volume_municipio.py \
  reducer/reducer_maior_volume_municipio.py \
  "PERGUNTA: Qual município recebeu o maior volume de recursos em um mês específico? | RESPOSTA: Município com maior volume total APENAS para maio/2015 (201505)"

echo "Todos os jobs completos. Veja os .txt em $RESULTS_DIR/"
