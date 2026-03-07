from confluent_kafka.admin import AdminClient, NewTopic

def create_compacted_topic():
    """
    Cria o topico 'delivery-positions' no cluster Kafka com configuracoes 
    especificas para Log Compaction e alta disponibilidade.
    """
    
    # Configuracao do cliente apontando para as portas locais expostas pelo Docker no host.
    # O mapeamento externo utiliza as portas 29092 a 29094 para contornar o isolamento 
    # de rede padrao do Docker no Windows.
    admin_client = AdminClient({
        "bootstrap.servers": "localhost:29092,localhost:29093,localhost:29094"
    })

    topic_name = "delivery-positions"
    
    # Configuracoes avancadas do topico:
    # cleanup.policy: 'compact' garante que o Kafka mantenha apenas a ultima mensagem 
    # para uma mesma chave (ex: ultimo GPS de um entregador), economizando disco.
    # min.insync.replicas: '2' garante que o cluster continue operando mesmo se 
    # um dos tres brokers ficar indisponivel.
    topic_config = {
        "cleanup.policy": "compact",
        "min.insync.replicas": "2"
    }

    # Instanciacao do objeto do topico com 3 particoes (para paralelismo) 
    # e fator de replicacao 3 (para seguranca dos dados).
    new_topic = NewTopic(
        topic=topic_name,
        num_partitions=3,
        replication_factor=3,
        config=topic_config
    )

    print(f"Iniciando requisicao para criacao do topico '{topic_name}'...")
    
    # Envio da requisicao assincrona para o cluster Kafka.
    fs = admin_client.create_topics([new_topic])

    # Aguarda a conclusao da operacao e trata o retorno.
    # Caso o topico ja exista, uma excecao e capturada e exibida como aviso.
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Sucesso: Topico '{topic}' criado com politica de Log Compaction.")
        except Exception as e:
            print(f"Aviso - Nao foi possivel criar o topico '{topic}': {e}")

if __name__ == "__main__":
    create_compacted_topic()