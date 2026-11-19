"""Generateur de transactions bancaires factices -> topic Kafka 'transactions'."""
import json, random, time, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

BOOTSTRAP = "localhost:29092"
TOPIC = "transactions"
FRAUD_RATE = 0.02          # ~2% de fraudes
RATE_PER_SEC = 20
COUNTRIES = ["MA", "FR", "ES", "US", "DE", "AE", "CN"]
MERCHANTS = ["grocery", "fuel", "restaurant", "online", "electronics", "travel", "luxury", "atm"]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def make_tx():
    is_fraud = random.random() < FRAUD_RATE
    home = "MA"
    now = datetime.now(timezone.utc)
    if is_fraud:
        # fraudes "realistes" : signaux souvent presents mais pas toujours (classes qui se chevauchent)
        amount = round(random.lognormvariate(5.0, 1.0), 2)
        country = random.choice([c for c in COUNTRIES if c != home]) if random.random() < 0.7 else home
        merchant = random.choice(["online", "electronics", "luxury", "travel"]) if random.random() < 0.6 else random.choice(MERCHANTS)
        hour = random.choice([0, 1, 2, 3, 4, 23]) if random.random() < 0.5 else random.randrange(24)
        device_new = random.random() < 0.5
    else:
        amount = round(random.lognormvariate(3.8, 1.0), 2)
        country = home if random.random() < 0.92 else random.choice(COUNTRIES)
        merchant = random.choice(MERCHANTS)
        hour = random.choices(range(24), weights=[1,1,1,1,1,2,3,5,6,6,6,7,8,7,6,6,6,7,8,7,5,3,2,1])[0]
        device_new = random.random() < 0.08
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 2000),
        "amount": amount,
        "merchant": merchant,
        "country": country,
        "home_country": home,
        "hour": hour,
        "device_new": device_new,
        "timestamp": now.isoformat(),
        "is_fraud": int(is_fraud),
    }

if __name__ == "__main__":
    print(f"Envoi vers {TOPIC} ({RATE_PER_SEC} tx/s) - Ctrl+C pour arreter")
    n = 0
    while True:
        producer.send(TOPIC, make_tx())
        n += 1
        if n % 200 == 0:
            print(f"{n} transactions envoyees")
        time.sleep(1 / RATE_PER_SEC)
