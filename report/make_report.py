"""Genere le rapport personnalise (HTML) a partir des resultats du batch et de l'entrainement.

Entrees (dossier reports/) : batch_summary.json, model_metrics.json, predictions_sample.csv
Sortie : reports/rapport_fraude.html  (ouvrir dans le navigateur, ou Imprimer -> Enregistrer en PDF)

Utilisation : python report/make_report.py [--reports-dir reports]
"""
import argparse, csv, html, json, os, urllib.error, urllib.request
from datetime import datetime

ap = argparse.ArgumentParser()
ap.add_argument("--reports-dir", default=os.getenv("REPORTS_DIR", "/opt/project/reports"))
ap.add_argument("--hdfs-url", default=os.getenv("WEBHDFS_URL", "http://namenode:9870"),
                help="adresse WebHDFS du namenode ; mettre 'none' pour ne pas envoyer dans HDFS")
ap.add_argument("--hdfs-dir", default="/lake/reports")
args = ap.parse_args()
D = args.reports_dir

def load_json(name):
    p = os.path.join(D, name)
    return json.load(open(p, encoding="utf-8")) if os.path.exists(p) else None

batch, models = load_json("batch_summary.json"), load_json("model_metrics.json")
preds = []
pp = os.path.join(D, "predictions_sample.csv")
if os.path.exists(pp):
    preds = list(csv.DictReader(open(pp, encoding="utf-8")))

e = html.escape

def table(headers, rows):
    th = "".join(f"<th>{e(str(h))}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{e(str(c))}</td>" for c in r) + "</tr>" for r in rows)
    return f"<table><thead><tr>{th}</tr></thead><tbody>{body}</tbody></table>"

def bar_chart(labels, values, title, color="#2563eb", unit=""):
    if not values:
        return ""
    w, h, pad = 560, 28 * len(values) + 20, 130
    mx = max(values) or 1
    out = [f'<svg width="{w}" height="{h}" role="img" aria-label="{e(title)}">']
    for i, (l, v) in enumerate(zip(labels, values)):
        y = 10 + i * 28
        bw = (w - pad - 70) * v / mx
        out.append(f'<text x="0" y="{y + 15}" font-size="12">{e(str(l))}</text>')
        out.append(f'<rect x="{pad}" y="{y}" width="{bw:.1f}" height="18" fill="{color}" rx="3"/>')
        out.append(f'<text x="{pad + bw + 6:.1f}" y="{y + 14}" font-size="12">{v:g}{unit}</text>')
    out.append("</svg>")
    return f"<h4>{e(title)}</h4>" + "".join(out)

parts = []
parts.append(f"""<h1>Rapport - Detection de fraude bancaire</h1>
<p class="muted">Genere le {datetime.now():%d/%m/%Y a %H:%M}</p>
<h2>1. Architecture</h2>
<p>Generateur Python &rarr; <b>Kafka</b> &rarr; <b>Spark Streaming</b> &rarr; <b>HDFS</b> (stockage distribue)
&rarr; <b>Spark Batch</b> (nettoyage, agregations) &rarr; <b>Spark ML</b> &rarr; <b>MLflow</b> (suivi, registre)
&rarr; <b>API de prediction</b>. Orchestration : Airflow / Kubeflow. Deploiement : Docker Compose.</p>""")

if batch:
    g = batch["global"]
    parts.append("<h2>2. Donnees et statistiques (traitement batch)</h2>")
    parts.append(table(["Indicateur", "Valeur"], [
        ["Lignes brutes / apres nettoyage", f'{batch["rows_raw"]} / {batch["rows_clean"]}'],
        ["Transactions", g["nb_tx"]], ["Clients distincts", g["nb_clients"]],
        ["Fraudes", f'{g["nb_fraudes"]} ({g["taux_fraude_pct"]} %)'],
        ["Montant moyen / max", f'{g["montant_moyen"]} / {g["montant_max"]}'],
        ["Periode", f'{g["debut"]} -> {g["fin"]}'],
    ]))
    parts.append("<h2>3. Agregations</h2><h3>Top 10 clients (montant total)</h3>")
    parts.append(table(["Client", "Nb tx", "Montant total", "Montant moyen", "Fraudes"],
                       [[r["user_id"], r["nb_tx"], r["montant_total"], r["montant_moyen"], r["nb_fraudes"]]
                        for r in batch["top_clients"]]))
    bm = batch["by_merchant"]
    parts.append("<h3>Par marchand</h3>" + table(
        ["Marchand", "Nb tx", "Montant moyen", "Fraudes", "Taux fraude %"],
        [[r["merchant"], r["nb_tx"], r["montant_moyen"], r["nb_fraudes"], r["taux_fraude_pct"]] for r in bm]))
    parts.append(bar_chart([r["merchant"] for r in bm], [r["taux_fraude_pct"] for r in bm],
                           "Taux de fraude par marchand", "#dc2626", " %"))
    bh = batch["by_hour"]
    parts.append(bar_chart([f'{int(r["hour"]):02d}h' for r in bh], [r["taux_fraude_pct"] for r in bh],
                           "Taux de fraude par heure", "#f59e0b", " %"))
else:
    parts.append("<h2>2. Donnees</h2><p>batch_summary.json introuvable : lance d'abord batch/batch_job.py.</p>")

if models:
    parts.append("<h2>4. Machine Learning</h2>")
    parts.append(f'<p>Donnees d\'entrainement : {models["rows"]} lignes dont {models["frauds"]} fraudes '
                 f'(classes desequilibrees, gerees par ponderation). Meilleur modele : '
                 f'<b>{e(models["best_model"])}</b> (AUC-PR = {models["best_auc_pr"]:.3f}).</p>')
    keys = ["precision", "recall", "f1", "auc_pr", "auc_roc"]
    parts.append(table(["Modele"] + keys, [[n] + [f"{m[k]:.3f}" for k in keys]
                                           for n, m in models["models"].items()]))
    parts.append(bar_chart(list(models["models"]), [round(m["auc_pr"], 3) for m in models["models"].values()],
                           "AUC-PR par modele"))
else:
    parts.append("<h2>4. Machine Learning</h2><p>model_metrics.json introuvable : lance spark/train.py.</p>")

if preds:
    parts.append("<h2>5. Exemples de predictions (20 plus risquees)</h2>")
    cols = list(preds[0].keys())
    parts.append(table(cols, [[r[c] for c in cols] for r in preds]))
    parts.append("<p class='muted'>label = verite terrain, prediction = decision du modele (1 = fraude).</p>")

style = """body{font-family:Segoe UI,Arial,sans-serif;max-width:900px;margin:30px auto;padding:0 16px;color:#111}
h1{color:#1e3a8a}h2{border-bottom:2px solid #e5e7eb;padding-bottom:4px;margin-top:32px}
table{border-collapse:collapse;width:100%;margin:10px 0;font-size:14px}
th,td{border:1px solid #d1d5db;padding:6px 8px;text-align:left}th{background:#f3f4f6}
.muted{color:#6b7280}svg{display:block;margin:6px 0 16px}"""
doc = f"<!DOCTYPE html><html lang='fr'><head><meta charset='utf-8'><title>Rapport fraude</title><style>{style}</style></head><body>{''.join(parts)}</body></html>"
os.makedirs(D, exist_ok=True)
out = os.path.join(D, "rapport_fraude.html")
open(out, "w", encoding="utf-8").write(doc)
print("Rapport genere :", out)


# ---- Envoi du rapport et des exports dans HDFS (via WebHDFS) ----
class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None

def _put(url, data=None):
    """PUT sans suivre la redirection ; renvoie (code, header Location)."""
    req = urllib.request.Request(url, data=data, method="PUT")
    if data is not None:
        req.add_header("Content-Type", "application/octet-stream")
    try:
        with urllib.request.build_opener(_NoRedirect).open(req, timeout=30) as r:
            return r.status, r.headers.get("Location")
    except urllib.error.HTTPError as e:
        if e.code in (301, 302, 307):
            return e.code, e.headers.get("Location")
        raise

def upload_to_hdfs(paths, base_url, hdfs_dir):
    base = base_url.rstrip("/") + "/webhdfs/v1"
    _put(f"{base}{hdfs_dir}?op=MKDIRS&user.name=root")
    for path in paths:
        name = os.path.basename(path)
        code, location = _put(f"{base}{hdfs_dir}/{name}?op=CREATE&overwrite=true&user.name=root")
        if not location:
            raise RuntimeError(f"WebHDFS n'a pas donne d'adresse datanode pour {name}")
        _put(location, data=open(path, "rb").read())
        print(f"  envoye dans HDFS : {hdfs_dir}/{name}")

if args.hdfs_url.lower() != "none":
    files = [out] + [os.path.join(D, f) for f in ("batch_summary.json", "model_metrics.json", "predictions_sample.csv")
                     if os.path.exists(os.path.join(D, f))]
    try:
        upload_to_hdfs(files, args.hdfs_url, args.hdfs_dir)
        print(f"Rapport stocke dans HDFS : {args.hdfs_dir}")
    except Exception as e:
        print(f"Envoi dans HDFS impossible ({e}). Le rapport reste disponible dans {D}.")
