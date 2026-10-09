# -*- mode: ruby -*-
# vi: set ft=ruby :
# ==============================================================================
# Vagrantfile — Sistema Distribuído Hospitalar em VMs Separadas
# ==============================================================================
#
# Cria 6 máquinas virtuais isoladas para demonstrar o sistema distribuído
# com middleware RabbitMQ em um ambiente próximo do real:
#
#   VM              IP              Componentes
#   ─────────────── ─────────────── ────────────────────────────────
#   vm-cliente      192.168.56.10   Leitor CSV + Produtor AMQP
#   vm-middleware   192.168.56.11   RabbitMQ Server
#   vm-balanceador  192.168.56.12   Consumidor AMQP + Balanceador
#   vm-servidor1    192.168.56.13   Servidor 01 (FastAPI :8001) + SQLite
#   vm-servidor2    192.168.56.14   Servidor 02 (FastAPI :8002) + SQLite
#   vm-servidor3    192.168.56.15   Servidor 03 (FastAPI :8003) + SQLite
#
# Uso:
#   vagrant up                  # Cria e provisiona todas as VMs
#   vagrant ssh vm-cliente      # Acessar a VM do cliente
#   vagrant ssh vm-middleware   # Acessar a VM do RabbitMQ
#   vagrant halt                # Desligar todas as VMs
#   vagrant destroy             # Destruir todas as VMs
#
# ==============================================================================

Vagrant.configure("2") do |config|

  # Box base: Ubuntu 22.04 LTS (leve e estável)
  config.vm.box = "ubuntu/jammy64"

  # Configuração global do VirtualBox
  config.vm.provider "virtualbox" do |vb|
    vb.memory = "512"
    vb.cpus = 1
    vb.gui = false
  end

  # ── Script de provisionamento comum ──────────────────────────
  # Instala Python 3.11, pip e copia o projeto
  PROVISION_COMMON = <<-SHELL
    set -e
    export DEBIAN_FRONTEND=noninteractive

    # Atualizar repositórios
    apt-get update -qq

    # Instalar Python 3 e pip
    apt-get install -y -qq python3 python3-pip python3-venv curl > /dev/null

    # Criar diretório do projeto
    mkdir -p /opt/projeto-sd
    cp -r /vagrant/* /opt/projeto-sd/ 2>/dev/null || true

    # Instalar dependências Python
    cd /opt/projeto-sd
    pip3 install -q -r requirements.txt 2>/dev/null || true

    # Criar diretórios
    mkdir -p databases logs data

    echo "=== Provisionamento comum concluído ==="
  SHELL

  # ────────────────────────────────────────────────────────────
  # VM 1: CLIENTE (Leitor CSV + Produtor AMQP)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-cliente" do |cliente|
    cliente.vm.hostname = "vm-cliente"
    cliente.vm.network "private_network", ip: "192.168.56.10"

    cliente.vm.provider "virtualbox" do |vb|
      vb.name = "SD-Cliente"
      vb.memory = "512"
    end

    cliente.vm.synced_folder ".", "/vagrant"

    cliente.vm.provision "shell", inline: PROVISION_COMMON

    cliente.vm.provision "shell", inline: <<-SHELL
      set -e
      echo "=== VM-CLIENTE configurada ==="
      echo "  IP: 192.168.56.10"
      echo "  Papel: Leitor CSV + Produtor AMQP"
      echo ""
      echo "  Para enviar dados via middleware:"
      echo "    cd /opt/projeto-sd"
      echo "    RABBITMQ_HOST=192.168.56.11 python3 middleware/produtor.py --limite 20 --intervalo 0.3"
      echo ""
      echo "  Para enviar via HTTP direto (sem middleware):"
      echo "    cd /opt/projeto-sd"
      echo "    python3 client/csv_sender.py --url http://192.168.56.12:8000/dados --limite 20 --intervalo 0.3"
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 2: MIDDLEWARE (RabbitMQ)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-middleware" do |middleware|
    middleware.vm.hostname = "vm-middleware"
    middleware.vm.network "private_network", ip: "192.168.56.11"

    middleware.vm.provider "virtualbox" do |vb|
      vb.name = "SD-Middleware-RabbitMQ"
      vb.memory = "1024"   # RabbitMQ precisa de mais memória
    end

    middleware.vm.provision "shell", inline: <<-SHELL
      set -e
      export DEBIAN_FRONTEND=noninteractive

      echo "=== Instalando RabbitMQ ==="

      # Instalar Erlang e RabbitMQ
      apt-get update -qq
      apt-get install -y -qq curl gnupg apt-transport-https > /dev/null

      # Adicionar repositório do RabbitMQ (via Cloudsmith)
      curl -1sLf 'https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-erlang/setup.deb.sh' | bash > /dev/null 2>&1
      curl -1sLf 'https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-server/setup.deb.sh' | bash > /dev/null 2>&1

      apt-get update -qq
      apt-get install -y -qq erlang-base erlang-nox rabbitmq-server > /dev/null

      # Habilitar plugin de management (interface web)
      rabbitmq-plugins enable rabbitmq_management > /dev/null 2>&1

      # Configurar para escutar em todas as interfaces
      cat > /etc/rabbitmq/rabbitmq.conf <<EOF
listeners.tcp.default = 5672
management.tcp.port = 15672
management.tcp.ip = 0.0.0.0
EOF

      # Reiniciar e habilitar no boot
      systemctl restart rabbitmq-server
      systemctl enable rabbitmq-server

      # Criar usuário admin para acesso remoto
      rabbitmqctl add_user admin admin123 2>/dev/null || true
      rabbitmqctl set_user_tags admin administrator 2>/dev/null || true
      rabbitmqctl set_permissions -p / admin ".*" ".*" ".*" 2>/dev/null || true

      # Permitir acesso guest de qualquer host (para desenvolvimento)
      rabbitmqctl set_permissions -p / guest ".*" ".*" ".*" 2>/dev/null || true
      cat >> /etc/rabbitmq/rabbitmq.conf <<EOF
loopback_users = none
EOF
      systemctl restart rabbitmq-server

      echo ""
      echo "=== VM-MIDDLEWARE configurada ==="
      echo "  IP: 192.168.56.11"
      echo "  Papel: RabbitMQ Server (Middleware MOM)"
      echo "  AMQP:  192.168.56.11:5672"
      echo "  Web UI: http://192.168.56.11:15672"
      echo "  Credenciais: guest/guest ou admin/admin123"
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 3: BALANCEADOR (Consumidor AMQP + Load Balancer)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-balanceador" do |balanceador|
    balanceador.vm.hostname = "vm-balanceador"
    balanceador.vm.network "private_network", ip: "192.168.56.12"

    balanceador.vm.provider "virtualbox" do |vb|
      vb.name = "SD-Balanceador"
      vb.memory = "512"
    end

    balanceador.vm.synced_folder ".", "/vagrant"

    balanceador.vm.provision "shell", inline: PROVISION_COMMON

    balanceador.vm.provision "shell", inline: <<-SHELL
      set -e
      echo "=== VM-BALANCEADOR configurada ==="
      echo "  IP: 192.168.56.12"
      echo "  Papel: Consumidor AMQP + Balanceador Round Robin"
      echo ""
      echo "  Para iniciar o balanceador:"
      echo "    cd /opt/projeto-sd"
      echo "    python3 load_balancer/balancer.py --port 8000 --servidores http://192.168.56.13:8001,http://192.168.56.14:8002,http://192.168.56.15:8003"
      echo ""
      echo "  Para iniciar o consumidor AMQP:"
      echo "    cd /opt/projeto-sd"
      echo "    RABBITMQ_HOST=192.168.56.11 python3 middleware/consumidor.py --lb-url http://localhost:8000"
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 4: SERVIDOR 01 (FastAPI :8001 + SQLite server1.db)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-servidor1" do |s1|
    s1.vm.hostname = "vm-servidor1"
    s1.vm.network "private_network", ip: "192.168.56.13"

    s1.vm.provider "virtualbox" do |vb|
      vb.name = "SD-Servidor-01"
      vb.memory = "512"
    end

    s1.vm.synced_folder ".", "/vagrant"
    s1.vm.provision "shell", inline: PROVISION_COMMON

    s1.vm.provision "shell", inline: <<-SHELL
      set -e
      echo "=== VM-SERVIDOR-01 configurada ==="
      echo "  IP: 192.168.56.13"
      echo "  Papel: Servidor 01 (FastAPI :8001) + SQLite server1.db"
      echo ""
      echo "  Para iniciar o servidor:"
      echo "    cd /opt/projeto-sd"
      echo "    python3 server/app.py --port 8001 --server-id server-01 --db-path databases/server1.db"
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 5: SERVIDOR 02 (FastAPI :8002 + SQLite server2.db)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-servidor2" do |s2|
    s2.vm.hostname = "vm-servidor2"
    s2.vm.network "private_network", ip: "192.168.56.14"

    s2.vm.provider "virtualbox" do |vb|
      vb.name = "SD-Servidor-02"
      vb.memory = "512"
    end

    s2.vm.synced_folder ".", "/vagrant"
    s2.vm.provision "shell", inline: PROVISION_COMMON

    s2.vm.provision "shell", inline: <<-SHELL
      set -e
      echo "=== VM-SERVIDOR-02 configurada ==="
      echo "  IP: 192.168.56.14"
      echo "  Papel: Servidor 02 (FastAPI :8002) + SQLite server2.db"
      echo ""
      echo "  Para iniciar o servidor:"
      echo "    cd /opt/projeto-sd"
      echo "    python3 server/app.py --port 8002 --server-id server-02 --db-path databases/server2.db"
    SHELL
  end

  # ────────────────────────────────────────────────────────────
  # VM 6: SERVIDOR 03 (FastAPI :8003 + SQLite server3.db)
  # ────────────────────────────────────────────────────────────
  config.vm.define "vm-servidor3" do |s3|
    s3.vm.hostname = "vm-servidor3"
    s3.vm.network "private_network", ip: "192.168.56.15"

    s3.vm.provider "virtualbox" do |vb|
      vb.name = "SD-Servidor-03"
      vb.memory = "512"
    end

    s3.vm.synced_folder ".", "/vagrant"
    s3.vm.provision "shell", inline: PROVISION_COMMON

    s3.vm.provision "shell", inline: <<-SHELL
      set -e
      echo "=== VM-SERVIDOR-03 configurada ==="
      echo "  IP: 192.168.56.15"
      echo "  Papel: Servidor 03 (FastAPI :8003) + SQLite server3.db"
      echo ""
      echo "  Para iniciar o servidor:"
      echo "    cd /opt/projeto-sd"
      echo "    python3 server/app.py --port 8003 --server-id server-03 --db-path databases/server3.db"
    SHELL
  end

end
