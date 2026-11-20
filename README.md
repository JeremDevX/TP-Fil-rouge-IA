# MAA — Kit de démarrage du TP fil rouge
Mutuelle Alpine Assurances est **fictive** ; les données **synthétiques** se génèrent localement dans `data/raw/`.
Suivez le [cahier de TP](supports/03_cahier_TP_fil_rouge.pdf) lot par lot. Les fichiers marqués `TODO` sont à écrire par votre équipe.
Infrastructure fournie : `docker-compose.yml` (profils), `requirements.txt`, `Makefile`, configuration Prometheus/Grafana, producteur Kafka.
Livrable final : voir cahier de TP §« Livrable » — sans oublier `docs/USAGE-IA.TXT`.

Consultez les [consignes d’installation](docs/INSTALL.md) et les [supports de cours](supports/).
Dans les supports d’origine, le chemin `maa-fraude-starter/` désigne la racine de ce dépôt.

## Démarrage local

Prérequis : Docker avec Compose, Python 3.11 et uv.

```bash
uv venv --python 3.11
uv pip install --python .venv/bin/python -r requirements.txt
uv pip check --python .venv/bin/python
make data PY=.venv/bin/python
docker compose up -d kafka
docker compose ps kafka
```

Le broker Kafka doit apparaître avec le statut `Up`. Le L0 se poursuit avec le schéma d’architecture, les critères de succès et la première entrée du [journal IA](docs/USAGE-IA.TXT).

Les données générées restent locales. Pour le lot RAG, récupérez le guide signalé « diffusion restreinte » dans le kit de cours et placez-le localement dans `data/docs/`.

## Fiches de synthèse

Chaque lot a sa fiche dans [docs/syntheses](docs/syntheses/), avec les étapes du cahier, les choix expliqués et les contrôles à reproduire.

- [L0 · Cadrer le projet et préparer le poste](docs/syntheses/L0-cadrage-installation.md)
- [L1 · Amener les données brutes jusqu’à DuckDB](docs/syntheses/L1-ingestion.md)

## Contribution

Proposez les changements sur une branche dédiée et ouvrez une pull request vers `main`.

Kit de départ : [supports et code fournis pour le cours](https://github.com/Laudroid/Live_Gouvernance_modelisation).
