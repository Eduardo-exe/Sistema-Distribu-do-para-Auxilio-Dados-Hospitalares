# -*- mode: ruby -*-
# vi: set ft=ruby :
# ==============================================================================
# Vagrantfile — Cluster de Máquinas Virtuais Reais (Sistema Distribuído Hospitalar)
# ==============================================================================
#
# Cria as 8 máquinas virtuais isoladas com Ubuntu 22.04 LTS interconectadas em 
# rede privada dedicada (192.168.56.0/24), reproduzindo um datacenter hospitalar real:
#
#   VM              IP              Função / Serviço Distribuído
#   ─────────────── ─────────────── ──────────────────────────────────────────
#   vm-cliente      192.168.56.10   Produtor Kafka (Leitor do Dataset CSV)
#   vm-middleware   192.168.56.11   Broker Apache Kafka (KRaft :9092)
#   vm-balanceador  192.168.56.12   Load Balancer Enterprise (:8000) + Consumidores
#   vm-servidor1    192.168.56.13   Servidor 01 (FastAPI :8001) + Shard server1.db
#   vm-servidor2    192.168.56.14   Servidor 02 (FastAPI :8002) + Shard server2.db
#   vm-servidor3    192.168.56.15   Servidor 03 (FastAPI :8003) + Shard server3.db
#   vm-servidor4    192.168.56.16   Servidor 04 (FastAPI :8004) + Shard server4.db (Warm Standby)
#   vm-monitor      192.168.56.17   Monitor SSE & Painel Web (:5000)
#
# Comandos principais:
#   vagrant up                     # Sobe e provisiona todas as 8 VMs
#   vagrant up vm-servidor1        # Sobe apenas o Servidor 01
#   vagrant ssh vm-servidor1       # Acessa o shell da VM do Servidor 01
#   vagrant halt                   # Desliga graciosamente todas as VMs
#   vagrant destroy -f             # Destrói e limpa as VMs
#
# ==============================================================================

Vagrant.configure("2") do |config|

  # Imagem base oficial do Ubuntu 22.04 LTS (Jammy Jellyfish)
  config.vm.box = "ubuntu/jammy64"

  # Configuração de hardware padrão para cada VM no VirtualBox
  config.vm.provider "virtualbox" do |vb|
    vb.memory = "512"
    vb.cpus = 1
    vb.gui = false
    vb.customize ["modifyvm", :id, "--nicpromisc2", "allow-all"]
  end

  # Script de provisionamento comum para nós Python
  PROVISION_COMMON = <<-SHELL
    set -e
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    apt-get install -y -qq python3 python3-pip python3-venv curl sqlite3 net-tools > /dev/null

    mkdir -p /opt/projeto-sd
    cp -r /vagrant/* /opt/projeto-sd/ 2>/dev/null || true

    cd /opt/projeto-sd
    python3 -m pip install -q -r requirements.txt 2>/dev/null || true
    mkdir -p databases logs data
    echo "=== Provisionamento comum concluído com sucesso ==="
  SHELL

  # ────────────────────────────────────────────────────────────
  # VM 1: CLIENTE (Leitor CSV + Produtor Kafka)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-cliente" do |cliente|
    cliente.vm.hostname = "vm-cliente"
    cliente.vm.network "private_network", ip: "192.168.56.10"
    cliente.vm.provider "virtualbox" do |vb|
      vb.name = "SD-VM-Cliente"
      vb.memory = "512"
    end
    cliente.vm.synced_folder ".", "/vagrant"
    cliente.vm.provision "shell", inline: PROVISION_COMMON
    cliente.vm.provision "shell", inline: <<-SHELL
      echo "=== VM-CLIENTE PRONTA (192.168.56.10) ==="
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 2: MIDDLEWARE (Apache Kafka KRaft)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-middleware" do |middleware|
    middleware.vm.hostname = "vm-middleware"
    middleware.vm.network "private_network", ip: "192.168.56.11"
    middleware.vm.provider "virtualbox" do |vb|
      vb.name = "SD-VM-Middleware-Kafka"
      vb.memory = "1024"
      vb.cpus = 2
    end
    middleware.vm.provision "shell", inline: <<-SHELL
      set -e
      export DEBIAN_FRONTEND=noninteractive
      apt-get update -qq
      apt-get install -y -qq openjdk-17-jre-headless curl net-tools > /dev/null

      # Baixar e instalar Apache Kafka oficial
      KAFKA_VERSION="3.7.0"
      SCALA_VERSION="2.13"
      if [ ! -d "/opt/kafka" ]; then
        curl -s "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -o /tmp/kafka.tgz
        tar -xzf /tmp/kafka.tgz -C /opt
        mv /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka
        rm /tmp/kafka.tgz
      fi

      # Formatar storage KRaft se necessário
      cd /opt/kafka
      KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-cluster-id)
      bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties --ignore-formatted || true

      # Ajustar listeners para escutar em todas as interfaces
      sed -i 's|PLAINTEXT://:9092|PLAINTEXT://0.0.0.0:9092|g' config/kraft/server.properties
      sed -i 's|advertised.listeners=PLAINTEXT://localhost:9092|advertised.listeners=PLAINTEXT://192.168.56.11:9092|g' config/kraft/server.properties

      # Iniciar como servico de background
      nohup bin/kafka-server-start.sh config/kraft/server.properties > /var/log/kafka.log 2>&1 &
      echo "=== VM-MIDDLEWARE PRONTA (Kafka KRaft ativo em 192.168.56.11:9092) ==="
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 3: BALANCEADOR (Load Balancer Enterprise)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-balanceador" do |balanceador|
    balanceador.vm.hostname = "vm-balanceador"
    balanceador.vm.network "private_network", ip: "192.168.56.12"
    balanceador.vm.provider "virtualbox" do |vb|
      vb.name = "SD-VM-Balanceador"
      vb.memory = "512"
    end
    balanceador.vm.synced_folder ".", "/vagrant"
    balanceador.vm.provision "shell", inline: PROVISION_COMMON
    balanceador.vm.provision "shell", inline: <<-SHELL
      echo "=== VM-BALANCEADOR PRONTA (192.168.56.12:8000) ==="
      echo "Iniciando balanceador enterprise..."
      cd /opt/projeto-sd
      nohup python3 load_balancer/balancer.py --port 8000 \
        --servidores http://192.168.56.13:8001,http://192.168.56.14:8002,http://192.168.56.15:8003 \
        --servidores-reserva http://192.168.56.16:8004 \
        --algorithm round_robin --auto-scale > /opt/projeto-sd/logs/balancer.log 2>&1 &
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 4: SERVIDOR 01 (FastAPI :8001 + SQLite server1.db)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-servidor1" do |s1|
    s1.vm.hostname = "vm-servidor-01"
    s1.vm.network "private_network", ip: "192.168.56.13"
    s1.vm.provider "virtualbox" do |vb|
      vb.name = "SD-VM-Servidor-01"
      vb.memory = "512"
    end
    s1.vm.synced_folder ".", "/vagrant"
    s1.vm.provision "shell", inline: PROVISION_COMMON
    s1.vm.provision "shell", inline: <<-SHELL
      echo "=== VM-SERVIDOR-01 PRONTA (192.168.56.13:8001) ==="
      cd /opt/projeto-sd
      nohup python3 server/app.py --port 8001 --server-id server-01 \
        --db-path databases/server1.db --vm-name vm-servidor-01 --vm-ip 192.168.56.13 > /opt/projeto-sd/logs/server1.log 2>&1 &
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 5: SERVIDOR 02 (FastAPI :8002 + SQLite server2.db)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-servidor2" do |s2|
    s2.vm.hostname = "vm-servidor-02"
    s2.vm.network "private_network", ip: "192.168.56.14"
    s2.vm.provider "virtualbox" do |vb|
      vb.name = "SD-VM-Servidor-02"
      vb.memory = "512"
    end
    s2.vm.synced_folder ".", "/vagrant"
    s2.vm.provision "shell", inline: PROVISION_COMMON
    s2.vm.provision "shell", inline: <<-SHELL
      echo "=== VM-SERVIDOR-02 PRONTA (192.168.56.14:8002) ==="
      cd /opt/projeto-sd
      nohup python3 server/app.py --port 8002 --server-id server-02 \
        --db-path databases/server2.db --vm-name vm-servidor-02 --vm-ip 192.168.56.14 > /opt/projeto-sd/logs/server2.log 2>&1 &
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 6: SERVIDOR 03 (FastAPI :8003 + SQLite server3.db)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-servidor3" do |s3|
    s3.vm.hostname = "vm-servidor-03"
    s3.vm.network "private_network", ip: "192.168.56.15"
    s3.vm.provider "virtualbox" do |vb|
      vb.name = "SD-VM-Servidor-03"
      vb.memory = "512"
    end
    s3.vm.synced_folder ".", "/vagrant"
    s3.vm.provision "shell", inline: PROVISION_COMMON
    s3.vm.provision "shell", inline: <<-SHELL
      echo "=== VM-SERVIDOR-03 PRONTA (192.168.56.15:8003) ==="
      cd /opt/projeto-sd
      nohup python3 server/app.py --port 8003 --server-id server-03 \
        --db-path databases/server3.db --vm-name vm-servidor-03 --vm-ip 192.168.56.15 > /opt/projeto-sd/logs/server3.log 2>&1 &
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 7: SERVIDOR 04 (Reserva Warm Standby :8004 + SQLite server4.db)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-servidor4" do |s4|
    s4.vm.hostname = "vm-servidor-04"
    s4.vm.network "private_network", ip: "192.168.56.16"
    s4.vm.provider "virtualbox" do |vb|
      vb.name = "SD-VM-Servidor-04-Reserva"
      vb.memory = "512"
    end
    s4.vm.synced_folder ".", "/vagrant"
    s4.vm.provision "shell", inline: PROVISION_COMMON
    s4.vm.provision "shell", inline: <<-SHELL
      echo "=== VM-SERVIDOR-04 PRONTA (192.168.56.16:8004 - RESERVA) ==="
      cd /opt/projeto-sd
      nohup python3 server/app.py --port 8004 --server-id server-04 \
        --db-path databases/server4.db --vm-name vm-servidor-04 --vm-ip 192.168.56.16 > /opt/projeto-sd/logs/server4.log 2>&1 &
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 8: MONITOR (Dashboard Web SSE :5000)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-monitor" do |monitor|
    monitor.vm.hostname = "vm-monitor"
    monitor.vm.network "private_network", ip: "192.168.56.17"
    monitor.vm.network "forwarded_port", guest: 5000, host: 5000
    monitor.vm.provider "virtualbox" do |vb|
      vb.name = "SD-VM-Monitor-Web"
      vb.memory = "512"
    end
    monitor.vm.synced_folder ".", "/vagrant"
    monitor.vm.provision "shell", inline: PROVISION_COMMON
    monitor.vm.provision "shell", inline: <<-SHELL
      echo "=== VM-MONITOR PRONTA (192.168.56.17:5000 -> Host :5000) ==="
      cd /opt/projeto-sd
      nohup python3 visualizacao/monitor.py > /opt/projeto-sd/logs/monitor.log 2>&1 &
    SHELL
  end

end
