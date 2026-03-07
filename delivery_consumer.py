import json
from confluent_kafka import Consumer

def main():
    """
    Inicializa um Consumidor Kafka para ler o topico de geolocalizacao
    e manter um estado atualizado dos entregadores em memoria.
    """
    
    conf = {
        'bootstrap.servers': 'localhost:29092,localhost:29093,localhost:29094',
        'group.id': 'painel-monitoramento-delivery',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    topic = "delivery-positions"
    
    # Inscreve o consumidor no topico
    consumer.subscribe([topic])

    print(f" Radar de Delivery ligado! Escutando o topico '{topic}'...")
    print("-" * 60)
    
    # Dicionario para armazenar o estado atual de cada entregador na nossa memoria local
    mapa_entregadores = {}

    try:
        while True:
            # Pede mensagens ao Kafka (timeout de 1 segundo)
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            if msg.error():
                print(f"Erro no consumidor: {msg.error()}")
                continue
            
            # Decodifica a chave (ID do motoboy) e o valor (o JSON com os dados)
            driver_key = msg.key().decode('utf-8')
            pos_data = json.loads(msg.value().decode('utf-8'))
            
            # Atualiza o nosso "mapa" local com a posicao mais recente
            mapa_entregadores[driver_key] = pos_data
            
            # Imprime na tela de forma amigavel a atualizacao que acabou de chegar
            status = pos_data['status']
            lat = pos_data['latitude']
            lon = pos_data['longitude']
            
            print(f" [ATUALIZACAO] Entregador {driver_key} | Status: {status:10} | GPS: ({lat}, {lon})")

    except KeyboardInterrupt:
        print("\nDesligando o radar de monitoramento...")
    finally:
        # Imprime o relatorio final de onde cada motoboy parou
        print("\n" + "=" * 40)
        print("🗺️  POSICAO FINAL CONHECIDA NO RADAR:")
        print("=" * 40)
        for driver, info in mapa_entregadores.items():
             print(f"- {driver}: {info['status']} em ({info['latitude']}, {info['longitude']})")
        
        # Fecha a conexao com o cluster de forma limpa
        consumer.close()

if __name__ == '__main__':
    main()