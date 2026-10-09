# middleware/__init__.py
"""
Camada de Middleware Orientado a Mensagens (MOM) com RabbitMQ.

Este pacote implementa o middleware de comunicação do sistema distribuído
hospitalar, utilizando o protocolo AMQP via RabbitMQ para desacoplar
temporalmente o produtor de telemetria dos servidores de processamento.

Componentes:
- config.py:     Configuração centralizada do RabbitMQ
- produtor.py:   Publicador AMQP (publica dados do CSV na fila)
- consumidor.py: Consumidor AMQP (consome da fila e encaminha ao balanceador)
"""
