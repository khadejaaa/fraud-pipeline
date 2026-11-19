"""Cycle de vie du modele dans le registre MLflow, avec VALIDATION HUMAINE du Senior Analyst (schema du prof :
"Assign model stage"). Un stage = un alias MLflow (staging, preprod) lu par les deploiements Kubernetes.

Circuit : Analyst (request) -> Senior Analyst (approve / reject) -> alias attribue -> "Prepare deployments" -> Git -> Argo CD

  python scripts/promote_model.py status
  python scripts/promote_model.py request  --alias staging --requester analyste1 [--version 2]
  python scripts/promote_model.py approve  --alias staging --approver senior1    [--version 2]
  python scripts/promote_model.py reject   --alias staging --approver senior1 --comment "recall trop bas"
  python scripts/promote_model.py request  --alias preprod --requester analyste1   # exige staging approuve avant

Regles : l'alias n'est JAMAIS pose sans `approve` ; l'approbateur doit etre different du demandeur ; `approve`
demande une confirmation tapee au clavier (docker exec -it ...). preprod n'est accessible qu'apres staging.
Dans docker : docker exec -it airflow python /opt/project/scripts/promote_model.py approve --alias staging --approver senior1
"""
import argparse, os, re, sys
from datetime import datetime, timezone
from pathlib import Path
from mlflow import MlflowClient

STAGES = ("staging", "preprod")          # 'champion' reste attribue automatiquement par l'entrainement (train.py)
PREV = {"staging": None, "preprod": "staging"}
HERE = Path(__file__).resolve().parent


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def tag(alias, key):
    return f"approval.{alias}.{key}"


def resolve_version(client, name, version):
    versions = client.search_model_versions(f"name='{name}'")
    if not versions:
        sys.exit(f"Aucun modele '{name}' dans le registre : lance d'abord l'entrainement.")
    if version is None:
        return max(int(v.version) for v in versions)
    if str(version) not in {v.version for v in versions}:
        sys.exit(f"Version {version} inconnue pour '{name}'.")
    return int(version)


def tags_of(client, name, version):
    return client.get_model_version(name, str(version)).tags


def cmd_status(client, a):
    print(f"Modele '{a.name}'")
    for v in sorted(client.search_model_versions(f"name='{a.name}'"), key=lambda x: int(x.version)):
        t = v.tags
        appr = ", ".join(f"{s}={t[tag(s, 'status')]}" + (f" (par {t[tag(s, 'by')]})" if tag(s, 'by') in t else "")
                         for s in STAGES if tag(s, "status") in t) or "-"
        print(f"  v{v.version:<3} alias: {','.join(sorted(client.get_model_version(a.name, v.version).aliases)) or '-':<22} approbations: {appr}")


def cmd_request(client, a):
    v = resolve_version(client, a.name, a.version)
    t = tags_of(client, a.name, v)
    prev = PREV[a.alias]
    if prev and t.get(tag(prev, "status")) != "approved":
        sys.exit(f"Refuse : la version {v} doit d'abord etre approuvee pour '{prev}' avant d'etre demandee pour '{a.alias}'.")
    client.set_model_version_tag(a.name, str(v), tag(a.alias, "status"), "pending")
    client.set_model_version_tag(a.name, str(v), tag(a.alias, "requested_by"), a.requester)
    client.set_model_version_tag(a.name, str(v), tag(a.alias, "requested_at"), now())
    for k in ("by", "at", "comment"):                       # une nouvelle demande annule une decision precedente
        if tag(a.alias, k) in t:
            client.delete_model_version_tag(a.name, str(v), tag(a.alias, k))
    print(f"Demande enregistree : '{a.name}' v{v} -> '{a.alias}' (en attente d'approbation du Senior Analyst)")


def prepare_deployment(alias, version, overlays_dir):
    """'Prepare deployments' du schema : ecrit la version validee dans l'overlay Kustomize de l'environnement.
    Le changement du ConfigMap fait changer son hash -> Argo CD redeploie les pods -> ils rechargent le modele."""
    f = Path(overlays_dir) / alias / "kustomization.yaml"
    if not f.exists():
        print(f"(deploiement non prepare : {f} introuvable ici. Lance la commande depuis le depot, ou utilise --overlays-dir)")
        return False
    txt = f.read_text(encoding="utf-8")
    line = f"      - MODEL_VERSION={version}"
    if re.search(r"^\s*- MODEL_VERSION=.*$", txt, re.M):
        txt = re.sub(r"^\s*- MODEL_VERSION=.*$", line, txt, flags=re.M)
    else:
        txt = re.sub(r"^(\s*- MODEL_URI=.*)$", r"\1\n" + line, txt, count=1, flags=re.M)
    f.write_text(txt, encoding="utf-8")
    print(f"Deploiement prepare : {f} (MODEL_VERSION={version})")
    print(f"  puis : git add {f} && git commit -m \"{alias}: modele v{version}\" && git push   (Argo CD synchronise ensuite)")
    return True


def cmd_approve(client, a):
    v = resolve_version(client, a.name, a.version)
    t = tags_of(client, a.name, v)
    if t.get(tag(a.alias, "status")) != "pending":
        sys.exit(f"Refuse : aucune demande en attente pour '{a.alias}' sur la version {v} "
                 f"(statut : {t.get(tag(a.alias, 'status'), 'aucune demande')}). L'Analyst doit lancer 'request' d'abord.")
    req = t.get(tag(a.alias, "requested_by"), "")
    if req.strip().lower() == a.approver.strip().lower():
        sys.exit(f"Refuse : '{a.approver}' a fait la demande, l'approbation doit venir d'une autre personne (Senior Analyst).")
    prev = PREV[a.alias]
    if prev and t.get(tag(prev, "status")) != "approved":
        sys.exit(f"Refuse : la version {v} n'est pas approuvee pour '{prev}'.")
    print(f"Modele '{a.name}' v{v} -> '{a.alias}' | demande par : {req} | approbateur : {a.approver}")
    print("Metriques de la version :", end=" ")
    try:
        run = client.get_run(client.get_model_version(a.name, str(v)).run_id)
        print({k: round(x, 3) for k, x in run.data.metrics.items()})
    except Exception:
        print("(indisponibles)")
    try:
        answer = input("Tape APPROUVER pour valider cette promotion (autre chose = annulation) : ").strip()
    except EOFError:
        sys.exit("Pas de saisie possible : lance la commande en mode interactif (docker exec -it ...). Rien n'a ete modifie.")
    if answer != "APPROUVER":
        sys.exit("Annule : aucune modification.")
    client.set_model_version_tag(a.name, str(v), tag(a.alias, "status"), "approved")
    client.set_model_version_tag(a.name, str(v), tag(a.alias, "by"), a.approver)
    client.set_model_version_tag(a.name, str(v), tag(a.alias, "at"), now())
    client.set_registered_model_alias(a.name, a.alias, str(v))
    print(f"APPROUVE : '{a.name}' v{v} -> alias '{a.alias}'")
    if not a.no_prepare:
        prepare_deployment(a.alias, v, a.overlays_dir)


def cmd_reject(client, a):
    v = resolve_version(client, a.name, a.version)
    t = tags_of(client, a.name, v)
    if t.get(tag(a.alias, "status")) != "pending":
        sys.exit(f"Refuse : aucune demande en attente pour '{a.alias}' sur la version {v}.")
    client.set_model_version_tag(a.name, str(v), tag(a.alias, "status"), "rejected")
    client.set_model_version_tag(a.name, str(v), tag(a.alias, "by"), a.approver)
    client.set_model_version_tag(a.name, str(v), tag(a.alias, "at"), now())
    if a.comment:
        client.set_model_version_tag(a.name, str(v), tag(a.alias, "comment"), a.comment)
    print(f"REJETE : '{a.name}' v{v} pour '{a.alias}'. Aucun alias modifie.")


def main():
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--name", default="fraud-detector")
    common.add_argument("--mlflow-uri", default=os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status", parents=[common])
    for c in ("request", "approve", "reject"):
        p = sub.add_parser(c, parents=[common])
        p.add_argument("--alias", required=True, choices=STAGES)
        p.add_argument("--version", type=int, default=None, help="defaut : derniere version")
        if c == "request":
            p.add_argument("--requester", required=True, help="nom de l'Analyst")
        else:
            p.add_argument("--approver", required=True, help="nom du Senior Analyst")
        if c == "reject":
            p.add_argument("--comment", default="")
        if c == "approve":
            p.add_argument("--no-prepare", action="store_true", help="ne pas modifier l'overlay Kustomize")
            p.add_argument("--overlays-dir", default=str(HERE.parent / "k8s" / "overlays"))
    a = ap.parse_args()
    client = MlflowClient(tracking_uri=a.mlflow_uri)
    {"status": cmd_status, "request": cmd_request, "approve": cmd_approve, "reject": cmd_reject}[a.cmd](client, a)


if __name__ == "__main__":
    main()
