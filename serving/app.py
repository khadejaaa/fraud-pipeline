"""API de prediction : sert le modele 'champion' du registre MLflow.

  GET  /health
  POST /predict   {"amount": 950, "merchant": "electronics", "country": "FR", "home_country": "MA", "hour": 3, "device_new": true}
  POST /reload    recharge le dernier modele champion
"""
import math, os, threading
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mlflow, mlflow.spark
from pyspark.sql import SparkSession

MODEL_URI = os.getenv("MODEL_URI", "models:/fraud-detector@champion")
THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.5"))

app = FastAPI(title="Fraud detection API")
_lock = threading.Lock()
_state = {"spark": None, "model": None}


class Transaction(BaseModel):
    amount: float
    merchant: str
    country: str
    home_country: str = "MA"
    hour: int
    device_new: bool = False


def _load():
    with _lock:
        if _state["spark"] is None:
            _state["spark"] = (SparkSession.builder.appName("fraud-api").master("local[1]")
                               .config("spark.ui.enabled", "false").getOrCreate())
        _state["model"] = mlflow.spark.load_model(MODEL_URI)
    return _state["model"]


def _model():
    if _state["model"] is None:
        try:
            _load()
        except Exception as e:
            raise HTTPException(503, f"Modele indisponible (lance l'entrainement d'abord) : {e}")
    return _state["model"]


@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": _state["model"] is not None, "model_uri": MODEL_URI}


@app.post("/reload")
def reload():
    _load()
    return {"status": "reloaded"}


@app.post("/predict")
def predict(tx: Transaction):
    model = _model()
    row = (tx.merchant, tx.amount, math.log1p(tx.amount), tx.hour,
           1 if (tx.hour >= 23 or tx.hour <= 5) else 0,
           int(tx.country != tx.home_country), int(tx.device_new))
    df = _state["spark"].createDataFrame(
        [row], ["merchant", "amount", "log_amount", "hour", "is_night", "is_foreign", "device_new"])
    r = model.transform(df).select("prediction", "probability").collect()[0]
    proba = float(r["probability"][1])
    return {"fraud_probability": round(proba, 4), "is_fraud": proba >= THRESHOLD,
            "model_prediction": int(r["prediction"])}
