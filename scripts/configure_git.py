"""Renseigne le vrai depot Git dans les 2 applications Argo CD (remplace le placeholder <TON-COMPTE>/<TON-DEPOT>).
  python scripts/configure_git.py --repo-url https://github.com/khadejaaa/fraud-pipeline.git [--branch main]
Argo CD lit k8s/overlays/<env> a la RACINE du depot : pousse le CONTENU du dossier fraud-pipeline/ (pas le dossier parent).
"""
import argparse, re, sys
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("--repo-url", required=True)
ap.add_argument("--branch", default="main")
a = ap.parse_args()
if not re.fullmatch(r"https://[\w.\-]+/[\w.\-]+/[\w.\-]+(\.git)?", a.repo_url):
    sys.exit("URL invalide : attendu https://github.com/<compte>/<depot>.git")
if "<" in a.repo_url:
    sys.exit("Remplace <compte>/<depot> par ton vrai depot.")
for env in ("staging", "preprod"):
    f = Path(__file__).resolve().parent.parent / "argocd" / f"application-{env}.yaml"
    t = f.read_text(encoding="utf-8")
    t = re.sub(r"^(\s*repoURL:).*$", rf"\1 {a.repo_url}", t, flags=re.M)
    t = re.sub(r"^(\s*targetRevision:).*$", rf"\1 {a.branch}", t, flags=re.M)
    f.write_text(t, encoding="utf-8")
    print("mis a jour :", f)
print(f"""
Ensuite (depuis le dossier du projet) :
  git init -b {a.branch}   # si pas deja fait
  git remote add origin {a.repo_url}
  git add . && git commit -m "pipeline fraude" && git push -u origin {a.branch}
  kubectl apply -f argocd/application-staging.yaml -f argocd/application-preprod.yaml
Depot prive : argocd repo add {a.repo_url} --username <user> --password <token>""")
