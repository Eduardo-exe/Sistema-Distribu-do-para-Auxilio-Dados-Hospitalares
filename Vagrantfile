# -*- mode: ruby -*-
# vi: set ft=ruby :
# ==============================================================================
# Vagrantfile — Cluster de Máquinas Virtuais Heterogêneas (Sistema Distribuído)
# ==============================================================================
#
# Demonstra na prática a TRANSPARÊNCIA DE HETEROGENEIDADE (Coulouris / Tanenbaum):
# O sistema opera de forma transparente entre diferentes distribuições Linux
# (Ubuntu 22.04 LTS Jammy, Debian 12 Bookworm, Ubuntu 20.04 LTS Focal), kernels e
# alocações de hardware, interconectadas na rede privada dedicada (192.168.56.0/24):
#
#   VM              IP              SO / Distribuição       Função / Serviço Distribuído
#   ─────────────── ─────────────── ─────────────────────── ──────────────────────────────────────────
#   vm-cliente      192.168.56.10   Ubuntu 22.04 LTS Jammy  Produtor Kafka (Leitor do Dataset CSV)
#   vm-middleware   192.168.56.11   Debian 12 Bookworm      Broker Apache Kafka (KRaft :9092)
#   vm-balanceador  192.168.56.12   Ubuntu 22.04 LTS Jammy  Load Balancer Enterprise (:8000)
#   vm-servidor1    192.168.56.13   Ubuntu 22.04 LTS Jammy  Servidor 01 (FastAPI :8001) + Shard server1.db
#   vm-servidor2    192.168.56.14   Debian 12 Bookworm      Servidor 02 (FastAPI :8002) + Shard server2.db
#   vm-servidor3    192.168.56.15   Ubuntu 20.04 LTS Focal  Servidor 03 (FastAPI :8003) + Shard server3.db
#   vm-servidor4    192.168.56.16   Debian 12 Bookworm      Servidor 04 (FastAPI :8004) + Shard server4.db (Warm Standby)
#   vm-monitor      192.168.56.17   Ubuntu 22.04 LTS Jammy  Monitor SSE & Painel Web (:5000)
#
# Comandos principais:
#   vagrant up                     # Sobe e provisiona todas as 8 VMs heterogêneas
#   vagrant up vm-servidor2        # Sobe especificamente o nó Debian 12
#   vagrant ssh vm-servidor2       # Acessa o shell da VM Debian 12
#   vagrant halt                   # Desliga graciosamente todas as VMs
#   vagrant destroy -f             # Destrói e limpa as VMs
#
# ==============================================================================

Vagrant.configure("2") do |config|

  # Imagem base padrão do cluster
  config.vm.box = "ubuntu/jammy64"

  # Configuração de hardware padrão para cada VM no VirtualBox
  config.vm.provider "virtualbox" do |vb|
    vb.memory = "512"
    vb.cpus = 1
    vb.gui = false
    vb.customize ["modifyvm", :id, "--nicpromisc2", "allow-all"]
  end

  # Script de provisionamento comum multiplataforma (Ubuntu 20/22 + Debian 12)
  PROVISION_COMMON = <<-SHELL
    set -e
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    apt-get install -y -qq python3 python3-pip python3-venv curl sqlite3 net-tools > /dev/null

    mkdir -p /opt/projeto-sd
    cp -r /vagrant/* /opt/projeto-sd/ 2>/dev/null || true

    cd /opt/projeto-sd
    # Criação de ambiente virtual isolado para compatibilidade Debian 12 (PEP 668) e Ubuntu
    python3 -m venv /opt/projeto-sd/venv 2>/dev/null || true
    if [ -f /opt/projeto-sd/venv/bin/pip ]; then
      /opt/projeto-sd/venv/bin/pip install -q -r requirements.txt 2>/dev/null || true
    else
      python3 -m pip install -q --break-system-packages -r requirements.txt 2>/dev/null || python3 -m pip install -q -r requirements.txt 2>/dev/null || true
    fi
    mkdir -p databases logs data
    echo "=== Provisionamento heterogêneo concluído com sucesso ($(cat /etc/os-release | grep PRETTY_NAME | cut -d= -f2)) ==="
  SHELL

  # ────────────────────────────────────────────────────────────
  # VM 1: CLIENTE (Ubuntu 22.04 LTS Jammy)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-cliente" do |cliente|
    cliente.vm.box = "ubuntu/jammy64"
    cliente.vm.hostname = "vm-cliente"
    cliente.vm.network "private_network", ip: "192.168.56.10"
    cliente.vm.provider "virtualbox" do |vb|
      vb.name = "SD-VM-Cliente-Ubuntu22"
      vb.memory = "512"
    end
    cliente.vm.synced_folder ".", "/vagrant"
    cliente.vm.provision "shell", inline: PROVISION_COMMON
    cliente.vm.provision "shell", inline: <<-SHELL
      echo "=== VM-CLIENTE PRONTA (Ubuntu 22.04 LTS @ 192.168.56.10) ==="
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 2: MIDDLEWARE KAFKA (Debian 12 Bookworm — Enterprise Broker)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-middleware" do |middleware|
    middleware.vm.box = "debian/bookworm64"
    middleware.vm.hostname = "vm-middleware"
    middleware.vm.network "private_network", ip: "192.168.56.11"
    middleware.vm.provider "virtualbox" do |vb|
      vb.name = "SD-VM-Middleware-Debian12"
      vb.memory = "1024"
      vb.cpus = 2
    end
    middleware.vm.provision "shell", inline: <<-SHELL
      set -e
      export DEBIAN_FRONTEND=noninteractive
      apt-get update -qq
      apt-get install -y -qq default-jre-headless curl net-tools procps > /dev/null

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

      # Iniciar como serviço de background
      nohup bin/kafka-server-start.sh config/kraft/server.properties > /var/log/kafka.log 2>&1 &
      echo "=== VM-MIDDLEWARE PRONTA (Debian 12 Bookworm + Kafka KRaft em 192.168.56.11:9092) ==="
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 3: BALANCEADOR (Ubuntu 22.04 LTS Jammy)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-balanceador" do |balanceador|
    balanceador.vm.box = "ubuntu/jammy64"
    balanceador.vm.hostname = "vm-balanceador"
    balanceador.vm.network "private_network", ip: "192.168.56.12"
    balanceador.vm.provider "virtualbox" do |vb|
      vb.name = "SD-VM-Balanceador-Ubuntu22"
      vb.memory = "512"
    end
    balanceador.vm.synced_folder ".", "/vagrant"
    balanceador.vm.provision "shell", inline: PROVISION_COMMON
    balanceador.vm.provision "shell", inline: <<-SHELL
      echo "=== VM-BALANCEADOR PRONTA (Ubuntu 22.04 LTS @ 192.168.56.12:8000) ==="
      cd /opt/projeto-sd
      PY_BIN="/opt/projeto-sd/venv/bin/python3"
      [ ! -f "$PY_BIN" ] && PY_BIN="python3"
      nohup "$PY_BIN" load_balancer/balancer.py --port 8000 \
        --servidores http://192.168.56.13:8001,http://192.168.56.14:8002,http://192.168.56.15:8003 \
        --servidores-reserva http://192.168.56.16:8004 \
        --algorithm round_robin --auto-scale > /opt/projeto-sd/logs/balancer.log 2>&1 &
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 4: SERVIDOR 01 (Ubuntu 22.04 LTS Jammy)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-servidor1" do |s1|
    s1.vm.box = "ubuntu/jammy64"
    s1.vm.hostname = "vm-servidor-01"
    s1.vm.network "private_network", ip: "192.168.56.13"
    s1.vm.provider "virtualbox" do |vb|
      vb.name = "SD-VM-Servidor-01-Ubuntu22"
      vb.memory = "512"
    end
    s1.vm.synced_folder ".", "/vagrant"
    s1.vm.provision "shell", inline: PROVISION_COMMON
    s1.vm.provision "shell", inline: <<-SHELL
      echo "=== VM-SERVIDOR-01 PRONTA (Ubuntu 22.04 LTS @ 192.168.56.13:8001) ==="
      cd /opt/projeto-sd
      PY_BIN="/opt/projeto-sd/venv/bin/python3"
      [ ! -f "$PY_BIN" ] && PY_BIN="python3"
      nohup "$PY_BIN" server/app.py --port 8001 --server-id server-01 \
        --db-path databases/server1.db --vm-name vm-servidor-01 --vm-ip 192.168.56.13 \
        --vm-os "Ubuntu 22.04 LTS (Jammy)" > /opt/projeto-sd/logs/server1.log 2>&1 &
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 5: SERVIDOR 02 (Debian 12 Bookworm — Heterogêneo)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-servidor2" do |s2|
    s2.vm.box = "debian/bookworm64"
    s2.vm.hostname = "vm-servidor-02"
    s2.vm.network "private_network", ip: "192.168.56.14"
    s2.vm.provider "virtualbox" do |vb|
      vb.name = "SD-VM-Servidor-02-Debian12"
      vb.memory = "512"
    end
    s2.vm.synced_folder ".", "/vagrant"
    s2.vm.provision "shell", inline: PROVISION_COMMON
    s2.vm.provision "shell", inline: <<-SHELL
      echo "=== VM-SERVIDOR-02 PRONTA (Debian 12 Bookworm @ 192.168.56.14:8002) ==="
      cd /opt/projeto-sd
      PY_BIN="/opt/projeto-sd/venv/bin/python3"
      [ ! -f "$PY_BIN" ] && PY_BIN="python3"
      nohup "$PY_BIN" server/app.py --port 8002 --server-id server-02 \
        --db-path databases/server2.db --vm-name vm-servidor-02 --vm-ip 192.168.56.14 \
        --vm-os "Debian 12 (Bookworm)" > /opt/projeto-sd/logs/server2.log 2>&1 &
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 6: SERVIDOR 03 (Ubuntu 20.04 LTS Focal — Heterogêneo)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-servidor3" do |s3|
    s3.vm.box = "ubuntu/focal64"
    s3.vm.hostname = "vm-servidor-03"
    s3.vm.network "private_network", ip: "192.168.56.15"
    s3.vm.provider "virtualbox" do |vb|
      vb.name = "SD-VM-Servidor-03-Ubuntu20"
      vb.memory = "512"
    end
    s3.vm.synced_folder ".", "/vagrant"
    s3.vm.provision "shell", inline: PROVISION_COMMON
    s3.vm.provision "shell", inline: <<-SHELL
      echo "=== VM-SERVIDOR-03 PRONTA (Ubuntu 20.04 LTS Focal @ 192.168.56.15:8003) ==="
      cd /opt/projeto-sd
      PY_BIN="/opt/projeto-sd/venv/bin/python3"
      [ ! -f "$PY_BIN" ] && PY_BIN="python3"
      nohup "$PY_BIN" server/app.py --port 8003 --server-id server-03 \
        --db-path databases/server3.db --vm-name vm-servidor-03 --vm-ip 192.168.56.15 \
        --vm-os "Ubuntu 20.04 LTS (Focal)" > /opt/projeto-sd/logs/server3.log 2>&1 &
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 7: SERVIDOR 04 (Debian 12 Bookworm — Reserva Warm Standby)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-servidor4" do |s4|
    s4.vm.box = "debian/bookworm64"
    s4.vm.hostname = "vm-servidor-04"
    s4.vm.network "private_network", ip: "192.168.56.16"
    s4.vm.provider "virtualbox" do |vb|
      vb.name = "SD-VM-Servidor-04-Debian12-Reserva"
      vb.memory = "512"
    end
    s4.vm.synced_folder ".", "/vagrant"
    s4.vm.provision "shell", inline: PROVISION_COMMON
    s4.vm.provision "shell", inline: <<-SHELL
      echo "=== VM-SERVIDOR-04 PRONTA (Debian 12 Bookworm @ 192.168.56.16:8004 - RESERVA) ==="
      cd /opt/projeto-sd
      PY_BIN="/opt/projeto-sd/venv/bin/python3"
      [ ! -f "$PY_BIN" ] && PY_BIN="python3"
      nohup "$PY_BIN" server/app.py --port 8004 --server-id server-04 \
        --db-path databases/server4.db --vm-name vm-servidor-04 --vm-ip 192.168.56.16 \
        --vm-os "Debian 12 (Bookworm)" > /opt/projeto-sd/logs/server4.log 2>&1 &
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 8: MONITOR (Ubuntu 22.04 LTS Jammy)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-monitor" do |monitor|
    monitor.vm.box = "ubuntu/jammy64"
    monitor.vm.hostname = "vm-monitor"
    monitor.vm.network "private_network", ip: "192.168.56.17"
    monitor.vm.network "forwarded_port", guest: 5000, host: 5000
    monitor.vm.provider "virtualbox" do |vb|
      vb.name = "SD-VM-Monitor-Web-Ubuntu22"
      vb.memory = "512"
    end
    monitor.vm.synced_folder ".", "/vagrant"
    monitor.vm.provision "shell", inline: PROVISION_COMMON
    monitor.vm.provision "shell", inline: <<-SHELL
      echo "=== VM-MONITOR PRONTA (Ubuntu 22.04 LTS @ 192.168.56.17:5000 -> Host :5000) ==="
      cd /opt/projeto-sd
      PY_BIN="/opt/projeto-sd/venv/bin/python3"
      [ ! -f "$PY_BIN" ] && PY_BIN="python3"
      nohup "$PY_BIN" visualizacao/monitor.py > /opt/projeto-sd/logs/monitor.log 2>&1 &
    SHELL
  end

end

