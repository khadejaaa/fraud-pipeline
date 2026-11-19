"""Teste les scripts des etapes Kubeflow en local (faux WebHDFS, echantillon Parquet, registre MLflow local).
  python tests/test_kubeflow_steps.py /tmp/sample file:/tmp/mlruns
"""
import json, subprocess, sys, threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

SAMPLE = sys.argv[1] if len(sys.argv) > 1 else "/tmp/sample"
URI = sys.argv[2] if len(sys.argv) > 2 else "file:/tmp/mlruns"
ROOT = Path(__file__).resolve().parent.parent
K = ROOT / "kubeflow" / "scripts"
TREE = {}  # chemin -> liste d'entrees, modifiable par les tests


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        p = self.path.split("?")[0].replace("/webhdfs/v1", "")
        if p not in TREE:
            self.send_response(404); self.end_headers(); return
        self.send_response(200); self.end_headers()
        self.wfile.write(json.dumps({"FileStatuses": {"FileStatus": TREE[p]}}).encode())

    def log_message(self, *a): pass


srv = HTTPServer(("127.0.0.1", 0), H)
threading.Thread(target=srv.serve_forever, daemon=True).start()
port = srv.server_address[1]
fails = 0


def run(cmd):
    r = subprocess.run([sys.executable, *cmd], capture_output=True, text=True)
    return r.returncode, r.stdout + r.stderr


def check(label, cond, out=""):
    global fails
    print(("OK   " if cond else "FAIL ") + label)
    if not cond:
        fails += 1; print("     ->", out.strip()[-300:])


chk = [str(K / "check_hdfs.py"), "--webhdfs-port", str(port)]
rc, o = run(chk + ["--data-path", "hdfs://127.0.0.1:9000/lake/transactions/"])
check("check_hdfs : dossier absent -> echec explicite", rc != 0 and "introuvable" in o, o)
TREE["/lake/transactions"] = []
rc, o = run(chk + ["--data-path", "hdfs://127.0.0.1:9000/lake/transactions/"])
check("check_hdfs : dossier vide -> echec explicite", rc != 0 and "aucun fichier" in o, o)
TREE["/lake/transactions"] = [{"pathSuffix": "_spark_metadata", "type": "DIRECTORY"}, {"pathSuffix": "date=2026-11-18", "type": "DIRECTORY"}]
TREE["/lake/transactions/date=2026-11-18"] = [{"pathSuffix": "part-0001.snappy.parquet", "type": "FILE"}]
rc, o = run(chk + ["--data-path", "hdfs://127.0.0.1:9000/lake/transactions/"])
check("check_hdfs : parquet dans une partition date= -> OK", rc == 0 and "1 fichiers" in o, o)
rc, o = run([str(K / "check_hdfs.py"), "--data-path", "hdfs://127.0.0.1:1/lake/transactions/", "--webhdfs-port", "1"])
check("check_hdfs : namenode injoignable -> message sur le reseau compose", rc != 0 and "reseau docker compose" in o, o)
rc, o = run([str(K / "check_hdfs.py"), "--data-path", "s3a://lake/x/"])
check("check_hdfs : chemin non-HDFS ignore", rc == 0, o)

rc, o = run([str(ROOT / "batch" / "batch_job.py"), "--data-path", SAMPLE, "--gold-path", "/tmp/gold_kf", "--reports-dir", "/tmp/rep_kf"])
check("prepare_data (batch_job.py) : nettoyage + agregations ecrites", rc == 0 and Path("/tmp/gold_kf/by_user").exists(), o)

rc, o = run([str(K / "verify_registry.py"), "--mlflow-uri", URI])
check("verify_registry : champion present", rc == 0 and "champion" in o, o)
rc, o = run([str(K / "verify_registry.py"), "--mlflow-uri", URI, "--model-name", "inexistant"])
check("verify_registry : modele absent -> echec", rc != 0, o)
print("\nRESULTAT :", "tous les tests passent" if fails == 0 else f"{fails} echec(s)")
sys.exit(1 if fails else 0)
