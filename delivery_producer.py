import json
from confluent_kafka import Producer
# Importa o gerador fornecido pelo professor
from generate_delivery_tracking import DeliveryTrackingGenerator
from dataclasses import asdict

def delivery_report(err, msg):
    """
    Callback executado confirmando o envio da mensagem ao broker.
    Informa sucesso ou falha no terminal.
    """
    if err is not None:
        print(f"Erro ao entregar mensagem: {err}")
    else:
        # Recupera a chave e a particao para monitoramento visual
        key = msg.key().decode('utf-8')
        print(f"Sucesso: [Topico: {msg.topic()} | Particao: {msg.partition()}] - Entregador: {key}")

def main():
    """
    Inicializa o produtor Kafka e consome o gerador de dados,
    enviando atualizacoes de geolocalizacao em tempo real.
    """
    
    # Configuracao do cliente conectando aos brokers mapeados localmente
    conf = {
        'bootstrap.servers': 'localhost:29092,localhost:29093,localhost:29094',
        'client.id': 'delivery-producer'
    }
    
    producer = Producer(conf)
    topic = "delivery-positions"

    # Inicializa o simulador do professor
    # Configuramos 5 motoboys e 2 atualizacoes por segundo para nao poluir demais a tela
    generator = DeliveryTrackingGenerator(updates_per_sec=2.0, num_drivers=5)

    print("Iniciando simulacao de telemetria. Pressione Ctrl+C para interromper.")
    
    try:
        # Itera sobre os dados gerados infinitamente
        for pos in generator.generate_tracking_data():
            # Converte o dataclass para dicionario padrao do Python
            pos_dict = asdict(pos)
            
            # Para o Log Compaction funcionar perfeitamente, a chave da mensagem 
            # DEVE ser obrigatoriamente um identificador unico do registro
            driver_key = str(pos.driver_id)
            
            # Serializa o dicionario em JSON e envia para o cluster
            producer.produce(
                topic=topic,
                key=driver_key.encode('utf-8'),
                value=json.dumps(pos_dict).encode('utf-8'),
                callback=delivery_report
            )
            
            # Chama eventos assincronos para garantir a execucao do callback
            producer.poll(0)
            
    except KeyboardInterrupt:
        print("\nSinal de interrupcao recebido. Finalizando produtor...")
    finally:
        # Aguarda a entrega de todas as mensagens que ainda estao em memoria
        print("Limpando buffer de mensagens...")
        producer.flush()

if __name__ == "__main__":
    main()