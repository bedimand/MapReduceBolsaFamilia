#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------
# Script de instalação, configuração e reset de HDFS + upload
# ------------------------------------------------------------

# Função de log com timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# Função para checar status de comando
check_status() {
    local status=$1; shift
    if [ "$status" -eq 0 ]; then
        log "✅ $*"
    else
        log "❌ $*"
        exit 1
    fi
}

# Garante que comandos hdfs/hadoop estejam disponíveis
ensure_hadoop_env() {
    if ! command -v hdfs &> /dev/null; then
        log "Carregando ambiente Hadoop..."
        source /etc/profile.d/hadoop.sh
        if ! command -v hdfs &> /dev/null; then
            log "❌ Falha ao carregar ambiente Hadoop"
            exit 1
        fi
    fi
}

# ==== 1. Variáveis gerais ====
HADOOP_VERSION="3.4.1"
HADOOP_URL="https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"
INSTALL_DIR="/opt"
HADOOP_HOME="${INSTALL_DIR}/hadoop-${HADOOP_VERSION}"
JAVA_PACKAGE="openjdk-11-jdk"

# ==== 1b. Variáveis HDFS e dados ====
LOCAL_PARQUET_DIR="/home/bedimand/bolsafamilia/parquet"
HDFS_TARGET_DIR="/user/bedimand/bolsa-familia/parquet"
HDFS_OUTPUT_DIR="/user/bedimand/bolsa-familia/output"

# ==== 2. Instalação de Pacotes Básicos ====
log "Atualizando repositórios e instalando pacotes necessários..."
sudo apt-get update -qq
check_status $? "apt-get update"
sudo apt-get install -y curl pv ${JAVA_PACKAGE}
check_status $? "Instalação de curl, pv e Java"

# detecta JAVA_HOME
JAVA_HOME_PATH=$(dirname "$(dirname "$(readlink -f "$(which java)")")")
log "JAVA_HOME detectado em ${JAVA_HOME_PATH}"

# ==== 3. Instalação do Hadoop ====
log "Verificando instalação do Hadoop..."
if [ ! -d "${HADOOP_HOME}" ]; then
    log "Baixando e instalando Hadoop ${HADOOP_VERSION}..."
    cd /tmp
    curl -L "${HADOOP_URL}" -o hadoop.tar.gz --progress-bar
    check_status $? "Download Hadoop"
    pv -t -e -r -a hadoop.tar.gz | sudo tar -xz -C "${INSTALL_DIR}"
    check_status $? "Extração Hadoop"
    sudo chown -R "$(whoami)":"$(whoami)" "${HADOOP_HOME}"
    sudo chmod -R 755 "${HADOOP_HOME}"
    rm hadoop.tar.gz
else
    log "Hadoop já instalado em ${HADOOP_HOME}"
fi

# ==== 4. Configuração de variáveis de ambiente ====
log "Configurando variáveis de ambiente do Hadoop..."
if [ ! -f "/etc/profile.d/hadoop.sh" ]; then
    sudo tee /etc/profile.d/hadoop.sh > /dev/null << EOF
export JAVA_HOME=${JAVA_HOME_PATH}
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=\${HADOOP_HOME}/etc/hadoop
export PATH=\$PATH:\${HADOOP_HOME}/bin:\${HADOOP_HOME}/sbin
EOF
    check_status $? "Criação do /etc/profile.d/hadoop.sh"
fi
source /etc/profile.d/hadoop.sh
log "hdfs OK"

# ==== 5. Configuração do HDFS (pseudo-distribuído) ====
log "Configurando core-site.xml e hdfs-site.xml..."
sudo mkdir -p "${HADOOP_HOME}/etc/hadoop"
sudo tee "${HADOOP_HOME}/etc/hadoop/core-site.xml" > /dev/null << EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://$(hostname):9000</value>
  </property>
</configuration>
EOF

sudo tee "${HADOOP_HOME}/etc/hadoop/hdfs-site.xml" > /dev/null << EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///var/hadoop/dfs/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///var/hadoop/dfs/data</value>
  </property>
  <property>
    <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
    <value>false</value>
  </property>
</configuration>
EOF

sudo mkdir -p /var/hadoop/dfs/name /var/hadoop/dfs/data
sudo chown -R "$(whoami)":"$(whoami)" /var/hadoop/dfs
check_status $? "Configuração HDFS no filesystem local"

# ==== 6. Reset completo do HDFS ====
ensure_hadoop_env

log "Parando daemons HDFS (se estiverem rodando)..."
if jps | grep -q NameNode; then
    hadoop-daemon.sh stop datanode; check_status $? "Stop DataNode"
    hadoop-daemon.sh stop namenode; check_status $? "Stop NameNode"
fi

log "Removendo dados antigos do HDFS no disco local..."
sudo rm -rf /var/hadoop/dfs/name/* /var/hadoop/dfs/data/*
check_status $? "Limpeza de diretórios locais do HDFS"

log "Formatando NameNode (reset HDFS)..."
hdfs namenode -format -force
check_status $? "Formatação NameNode"

log "Iniciando daemons HDFS..."
hadoop-daemon.sh start namenode; check_status $? "Start NameNode"
hadoop-daemon.sh start datanode;   check_status $? "Start DataNode"

log "Aguardando HDFS ficar pronto..."
sleep 10

# ==== 7. Upload dos Parquets para o HDFS ====
log "Criando diretório de destino no HDFS: ${HDFS_TARGET_DIR}"
hdfs dfs -mkdir -p "${HDFS_TARGET_DIR}"
check_status $? "Criação de diretório HDFS"

log "Enviando arquivos Parquet para o HDFS, particionando por mês..."
for pq_file in "${LOCAL_PARQUET_DIR}"/*.parquet; do
    [ -f "$pq_file" ] || continue
    fname=$(basename "$pq_file")
    mes=${fname:0:6}
    target="${HDFS_TARGET_DIR}/mes=${mes}"
    hdfs dfs -mkdir -p "${target}"
    check_status $? "mkdir HDFS ${target}"
    hdfs dfs -put -f "${pq_file}" "${target}/"
    check_status $? "put ${fname} → ${target}"
done

log "✅ HDFS resetado e upload de Parquets concluído!"
