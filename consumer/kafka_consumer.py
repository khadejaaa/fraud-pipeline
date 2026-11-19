"""Consumer Kafka : lit le topic 'transactions' pour VERIFIER que les messages arrivent.

Usage (sur ta machine, le generateur doit tourner) :
    python consumer/kafka_consumer.py              # affiche 20 messages puis s'arrete
    python consumer/kafka_consumer.py --max 100
"""
import argparse, json
from kafka import KafkaConsumer

ap = argparse.ArgumentParser()
ap.add_argument("--bootstrap", default="localhost:29092")
ap.add_argument("--topic", default="transactions")
ap.add_argument("--max", type=int, default=20, help="nombre de messages a lire")
args = ap.parse_args()

consumer = KafkaConsumer(
    args.topic,
    bootstrap_servers=args.bootstrap,
    auto_offset_reset="latest",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    consumer_timeout_ms=30000,
)

print(f"Lecture de {args.max} messages sur '{args.topic}' ({args.bootstrap}) ...")
n = frauds = 0
for msg in consumer:
    tx = msg.value
    n += 1
    frauds += tx.get("is_fraud", 0)
    print(f"[{n}] partition={msg.partition} offset={msg.offset} "
          f"user={tx['user_id']} montant={tx['amount']} marchand={tx['merchant']} "
          f"pays={tx['country']} fraude={tx['is_fraud']}")
    if n >= args.max:
        break

print(f"\nOK : {n} messages recus, dont {frauds} fraudes.")
if n == 0:
    print("Aucun message : verifie que kafka tourne et que fraud_gen.py est lance.")
