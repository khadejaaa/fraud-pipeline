"""Genere un echantillon Parquet au meme schema que la sortie de spark/streaming_job.py (tests locaux, sans Kafka/HDFS).
  python tests/make_sample_data.py /tmp/sample 6000
"""
import math, random, sys, uuid
from datetime import datetime, timedelta
import pandas as pd

out = sys.argv[1] if len(sys.argv) > 1 else "/tmp/sample"
n = int(sys.argv[2]) if len(sys.argv) > 2 else 6000
random.seed(1)
C = ["MA", "FR", "ES", "US", "DE", "AE", "CN"]
M = ["grocery", "fuel", "restaurant", "online", "electronics", "travel", "luxury", "atm"]
rows, t0 = [], datetime(2026, 11, 1)
for i in range(n):
    fr = random.random() < 0.04
    amount = round(random.lognormvariate(5.0 if fr else 3.8, 1.0), 2)
    country = random.choice(C[1:]) if (fr and random.random() < .7) else ("MA" if random.random() < .92 else random.choice(C))
    merchant = random.choice(["online", "electronics", "luxury", "travel"]) if (fr and random.random() < .6) else random.choice(M)
    hour = random.choice([0, 1, 2, 3, 4, 23]) if (fr and random.random() < .5) else random.randrange(24)
    dev = int(random.random() < (.5 if fr else .08))
    ts = t0 + timedelta(seconds=i * 30)
    rows.append(dict(transaction_id=str(uuid.uuid4()), user_id=random.randint(1, 2000), amount=amount, merchant=merchant,
                     country=country, home_country="MA", hour=hour, device_new=dev, timestamp=ts.isoformat(),
                     is_fraud=int(fr), event_ts=ts, is_foreign=int(country != "MA"),
                     is_night=int(hour >= 23 or hour <= 5), log_amount=math.log1p(amount), date=ts.date()))
df = pd.DataFrame(rows)
df.to_parquet(out, partition_cols=["date"], index=False, coerce_timestamps="ms", allow_truncated_timestamps=True)
print(f"{n} lignes ({int(df.is_fraud.sum())} fraudes) -> {out}")
