# Données du TP — Mutuelle Alpine Assurances (FICTIVES)
Depuis la racine, `make data PY=.venv/bin/python` génère ces six fichiers localement (seed 42).
| Fichier | Lignes | Contenu |
|---|---|---|
| assures.csv | 4 000 | assurés (données personnelles fictives : à traiter comme réelles) |
| contrats.csv | 4 909 | contrats auto / habitation |
| prestataires.csv | 120 | garages et artisans |
| enquetes.csv | 2 841 | conclusions des enquêtes anti-fraude |
| sinistres_events.jsonl | 12 080 | déclarations (flux Kafka à rejouer) |
| sinistres_events_2026_01.jsonl | 800 | lot de janvier 2026 — à n'utiliser qu'au lot L8 |
