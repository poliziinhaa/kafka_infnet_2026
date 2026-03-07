import urllib.request
import urllib.error
import json
import time

def setup_connector():
    """
    Script para automatizar a criacao do conector Kafka -> Redis.
    Envia o payload JSON de configuracao para a API REST do Kafka Connect.
    """
    url = "http://localhost:8083/connectors"
    headers = {'Content-Type': 'application/json'}
    
    # Configuracao do conector baseada nos parametros oficiais do Redis Sink
    payload = {
        "name": "redis-delivery-sink",
        "config": {
            "connector.class": "com.redis.kafka.connect.RedisSinkConnector",
            "redis.uri": "redis://my-redis:6379",
            "topics": "delivery-positions",
            "tasks.max": "1",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "redis.keyspace": "driver",
            "redis.type": "HASH",
            "errors.tolerance": "all"
        }
    }
    
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers=headers, method='POST')
    
    print("Aguardando Kafka Connect iniciar (isso pode levar ate 2 minutos)...")
    
    # Tenta conectar na API repetidamente ate o servico estar disponivel
    for _ in range(30):
        try:
            urllib.request.urlopen("http://localhost:8083/")
            break
        except Exception:
            time.sleep(5)
            print(".", end="", flush=True)
            
    print("\nKafka Connect online. Enviando configuracao do conector...")
    
    # Envia a requisicao e trata as possiveis respostas
    try:
        with urllib.request.urlopen(req) as response:
            res_body = response.read()
            print("Sucesso! Conector criado com as seguintes configuracoes:")
            print(json.dumps(json.loads(res_body.decode('utf-8')), indent=2))
    except urllib.error.HTTPError as e:
        err_msg = e.read().decode('utf-8')
        if "already exists" in err_msg:
            print("Aviso: O conector 'redis-delivery-sink' ja existe e esta rodando.")
        else:
            print(f"Erro HTTP {e.code}: {err_msg}")
    except Exception as e:
        print(f"Erro fatal ao conectar: {e}")

if __name__ == "__main__":
    setup_connector()