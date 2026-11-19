"""Etape 1 du pipeline Kubeflow : verifie que le streaming a bien ecrit des fichiers Parquet dans HDFS.
Utilise WebHDFS (port 9870 du namenode) : pas besoin d'atteindre les datanodes pour cette verification.
  python check_hdfs.py --data-path hdfs://namenode:9000/lake/transactions/
Sortie 0 = des fichiers .parquet existent ; sortie 1 = HDFS injoignable ou vide (message explicite).
"""
import argparse, json, sys, urllib.error, urllib.parse, urllib.request

ap = argparse.ArgumentParser()
ap.add_argument("--data-path", required=True)
ap.add_argument("--webhdfs-port", type=int, default=9870)
ap.add_argument("--min-files", type=int, default=1)
a = ap.parse_args()

if not a.data_path.startswith("hdfs://"):
    print(f"data_path n'est pas sur HDFS ({a.data_path}) : verification ignoree.")
    sys.exit(0)

u = urllib.parse.urlparse(a.data_path)
base = f"http://{u.hostname}:{a.webhdfs_port}/webhdfs/v1"


def ls(path):
    with urllib.request.urlopen(f"{base}{path}?op=LISTSTATUS", timeout=20) as r:
        return json.load(r)["FileStatuses"]["FileStatus"]


def count_parquet(path, depth=0):
    n = 0
    for it in ls(path):
        child = path.rstrip("/") + "/" + it["pathSuffix"]
        if it["type"] == "DIRECTORY" and depth < 3 and not it["pathSuffix"].startswith("_"):
            n += count_parquet(child, depth + 1)
        elif it["type"] == "FILE" and it["pathSuffix"].endswith(".parquet"):
            n += 1
    return n


try:
    n = count_parquet(u.path.rstrip("/") or "/")
except urllib.error.HTTPError as e:
    sys.exit(f"ECHEC : {u.path} introuvable dans HDFS (HTTP {e.code}). Le streaming a-t-il tourne ?")
except Exception as e:
    sys.exit(f"ECHEC : namenode '{u.hostname}' injoignable depuis ce pod ({e}). "
             f"Le cluster kind doit etre connecte au reseau docker compose (voir kubeflow/setup_kind.sh).")
if n < a.min_files:
    sys.exit(f"ECHEC : aucun fichier .parquet dans {u.path} : lance le generateur et laisse tourner le streaming.")
print(f"OK : {n} fichiers Parquet dans {a.data_path}")
