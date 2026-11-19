"""Test du circuit de validation (scripts/promote_model.py) contre un registre MLflow local.
Pre-requis : un modele 'fraud-detector' existe (python spark/train.py --data-path /tmp/sample --mlflow-uri file:/tmp/mlruns ...).
  python tests/test_promotion_workflow.py file:/tmp/mlruns
"""
import shutil, subprocess, sys, tempfile
from pathlib import Path

URI = sys.argv[1] if len(sys.argv) > 1 else "file:/tmp/mlruns"
ROOT = Path(__file__).resolve().parent.parent
tmp = Path(tempfile.mkdtemp())
shutil.copytree(ROOT / "k8s" / "overlays", tmp / "overlays")
fails = 0


def run(args, stdin=""):
    r = subprocess.run([sys.executable, str(ROOT / "scripts" / "promote_model.py"), args[0], "--mlflow-uri", URI, *args[1:]],
                       input=stdin, capture_output=True, text=True)
    return r.returncode, (r.stdout + r.stderr)


def check(label, cond, out=""):
    global fails
    print(("OK   " if cond else "FAIL ") + label)
    if not cond:
        fails += 1
        print("     ->", out.strip()[:300])


ov = ["--overlays-dir", str(tmp / "overlays")]
rc, o = run(["approve", "--alias", "staging", "--approver", "senior1", *ov], "APPROUVER\n")
check("approve sans demande est refuse", rc != 0 and "request" in o, o)
rc, o = run(["request", "--alias", "preprod", "--requester", "analyste1"])
check("preprod refuse tant que staging n'est pas approuve", rc != 0 and "staging" in o, o)
rc, o = run(["request", "--alias", "staging", "--requester", "analyste1"])
check("request staging enregistre une demande 'pending'", rc == 0 and "en attente" in o, o)
rc, o = run(["approve", "--alias", "staging", "--approver", "Analyste1", *ov], "APPROUVER\n")
check("auto-approbation refusee (meme personne)", rc != 0 and "autre personne" in o, o)
rc, o = run(["approve", "--alias", "staging", "--approver", "senior1", *ov], "non\n")
check("confirmation incorrecte = annulation, alias non pose", rc != 0 and "Annule" in o, o)
rc, o = run(["approve", "--alias", "staging", "--approver", "senior1", *ov], "")
check("sans saisie humaine (stdin vide) = refuse", rc != 0 and "interactif" in o, o)
rc, o = run(["status"])
alias_col = o.split("alias:")[1].split("approbations")[0]
check("avant approbation : alias staging absent (seul 'champion' de l'entrainement)", "staging" not in alias_col and "champion" in alias_col, o)
rc, o = run(["approve", "--alias", "staging", "--approver", "senior1", *ov], "APPROUVER\n")
check("approve par un autre Senior + saisie APPROUVER -> alias staging pose", rc == 0 and "APPROUVE" in o, o)
check("overlay staging mis a jour (MODEL_VERSION)", "MODEL_VERSION=1" in (tmp / "overlays/staging/kustomization.yaml").read_text())
check("overlay preprod NON modifie", "MODEL_VERSION=unset" in (tmp / "overlays/preprod/kustomization.yaml").read_text())
rc, o = run(["request", "--alias", "preprod", "--requester", "analyste1"])
check("request preprod accepte apres staging approuve", rc == 0, o)
rc, o = run(["reject", "--alias", "preprod", "--approver", "senior1", "--comment", "test"])
check("reject -> statut 'rejected', aucun alias preprod", rc == 0 and "REJETE" in o, o)
rc, o = run(["approve", "--alias", "preprod", "--approver", "senior1", *ov], "APPROUVER\n")
check("approve apres rejet refuse (nouvelle demande necessaire)", rc != 0, o)
run(["request", "--alias", "preprod", "--requester", "analyste1"])
rc, o = run(["approve", "--alias", "preprod", "--approver", "senior1", *ov], "APPROUVER\n")
check("nouvelle demande + approbation -> alias preprod", rc == 0 and "preprod" in o, o)
rc, o = run(["status"])
print(o)
alias_col = o.split("alias:")[1].split("approbations")[0]
check("status affiche les alias staging et preprod sur v1", "staging" in alias_col and "preprod" in alias_col, o)
print("\nRESULTAT :", "tous les tests passent" if fails == 0 else f"{fails} echec(s)")
sys.exit(1 if fails else 0)
