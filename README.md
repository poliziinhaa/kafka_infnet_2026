## Sistema de Telemetria de Delivery - Apache Kafka & KRaft

Este repositorio contem o projeto pratico final de Engenharia de Dados (Infnet 2026). O sistema simula uma operacao de delivery em tempo real, monitorando a geolocalizacao de entregadores e persistindo o estado atual em um banco de dados de alto desempenho.

## Arquitetura do Sistema

O projeto foi desenhado seguindo os principios de sistemas distribuidos e observabilidade:

1. **Cluster de Mensageria:** 3 Brokers Apache Kafka operando em modo KRaft (sem Zookeeper), garantindo alta disponibilidade e quorum de replicacao.
2. **Ingestao (Producer):** Script Python que simula o GPS de 5 entregadores simultaneos enviando dados em formato JSON.
3. **Processamento de Estado (Log Compaction):** Topico configurado com a politica `compact` para manter apenas a ultima coordenada conhecida de cada entregador.
4. **Persistencia (Sink):** Uso do Kafka Connect com o conector Redis Sink para espelhar as posicoes do Kafka para o Redis automaticamente.
5. **Observabilidade:** Pipeline de metricas utilizando Prometheus para coleta e Grafana para visualizacao.

---

## Tecnologias Utilizadas

* **Core:** Apache Kafka 4.1.1, Kafka Connect, Kafka UI.
* **Database:** Redis (In-memory storage).
* **Monitoring:** Prometheus & Grafana.
* **Language:** Python 3.x (confluent-kafka).
* **Infrastructure:** Docker & Docker Compose.

---

## Como Rodar o Projeto

### 1. Iniciar a Infraestrutura
Com o Docker ativo, execute o comando na raiz do projeto para subir os conteineres:

```bash
docker compose up -d
```

### 2. Configurar o Ambiente Python
```bash
python -m venv venv
.\venv\Scripts\activate
pip install confluent-kafka redis requests
```

### 3. Configurar o Tópico e o Conectar
```bash
# Cria o topico delivery-positions com Log Compaction
python setup_topics.py

# Configura a ponte Kafka Connect -> Redis
python setup_redis_sink.py
```

### 4. Executar a Telemetria
```bash
# Iniciar o envio de coordenadas GPS (Produtor)
python delivery_producer.py

# (Opcional) Monitorar leitura basica via terminal (Consumidor)
python delivery_consumer.py
```

## Monitoramento
O ambiente conta com monitoramento para validar a saúde do cluster.
* **Acesso Grafana**: http://localhost:3001
* **Credenciais**: Usuário admin | Senha admin
* **Dashboard** para visualizar as métricas

## Decisões Técnicas de Destaque
* **Log Compaction**: Escolhido para o topico de geolocalização. Em um aplicativo de delivery, a posicao histórica perde valor rapidamente comparada a posicao atual. O Log Compaction otimiza o armazenamento do Kafka, guardando apenas a última atualizacao de cada chave (ID do entregador).
* **Kafka Connect**: Optou-se pela utilizacao do framework Kafka Connect (Sink) para a persistência no Redis por ser uma solucão robusta, tolerante a falhas e nativa do ecossistema Kafka, eliminando a necessidade de gerenciar o ciclo de vida de um script consumidor avulso.










