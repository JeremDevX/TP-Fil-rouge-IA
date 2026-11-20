# L0 · Cadrer le projet et préparer le poste

La Mutuelle Alpine Assurances reçoit environ 4 000 déclarations de sinistre par an. Quatre enquêteurs doivent décider quels dossiers examiner en priorité. Imaginons que 100 déclarations valides arrivent cette semaine. Si le système en signale 60, qui pourra les examiner dans les délais ? Cette question de charge explique pourquoi le TP commence par définir le succès avant de construire un modèle.

À la fin de L0, l’équipe doit disposer d’un poste prêt pour l’ingestion, pouvoir suivre une déclaration sur un schéma et expliquer comment elle jugera le système utile. Les commandes ci-dessous se lancent depuis la racine du dépôt.

## 1. Former l’équipe et organiser les contributions

Le kit de départ fournit les fichiers communs du TP. Git conserve l’historique des modifications. La branche `main` représente la version partagée de l’équipe. Une branche de travail permet de préparer un changement, par exemple le chargement des déclarations, et de le tester avant son intégration.

Le parcours suit quatre gestes. Créer une branche pour le lot, enregistrer les modifications cohérentes dans un *commit*, ouvrir une demande de fusion (*pull request*) vers `main`, puis faire relire les changements par un autre membre. La protection de `main` fait appliquer les règles d’intégration sur GitHub. Pour la vérifier, consulter les règles de branche applicables à `main` et vérifier que la demande de fusion est exigée. La simple présence d’anciennes demandes de fusion prouve leur usage, mais ne suffit pas à établir que la règle est activée.

Le journal d’usage de l’intelligence artificielle (IA) commence dès ce cadrage. L’équipe y indique ses membres et consigne les usages significatifs, les propositions acceptées, corrigées ou refusées, les vérifications et le temps estimé. Par exemple, le journal du projet explique pourquoi la cible de rappel a été révisée. Cet exemple permet de raconter une décision en soutenance. Les estimations de temps et les appréciations restent à renseigner par leurs auteurs. [Journal d’usage](../USAGE-IA.TXT)

## 2. Installer les outils et reconnaître les six fichiers

Les scripts du cours utilisent plusieurs bibliothèques Python. Le dossier `.venv` les isole pour ce projet. Sur un poste neuf, la procédure du dépôt utilise Python 3.11 et l’outil d’installation `uv`.

```bash
uv venv --python 3.11
uv pip install --python .venv/bin/python -r requirements.txt
uv pip check --python .venv/bin/python
```

La première commande crée l’environnement, la deuxième y installe les bibliothèques du kit et la troisième vérifie leur compatibilité. Sur un poste déjà préparé, commencer par la troisième commande suffit pour ce contrôle.

| Fichier dans `data/raw/` | Contenu | Volume du jeu initial |
|---|---|---:|
| `assures.csv` | Personnes assurées | 4 000 |
| `contrats.csv` | Contrats et garanties | 4 909 |
| `prestataires.csv` | Garages et artisans | 120 |
| `enquetes.csv` | Suivi des enquêtes | 2 841 |
| `sinistres_events.jsonl` | Déclarations à envoyer au L1 | 12 080 |
| `sinistres_events_2026_01.jsonl` | Déclarations de janvier 2026 réservées au L8 | 800 |

Un CSV est un fichier de valeurs séparées par des virgules. Un fichier JSONL contient un objet JSON (*JavaScript Object Notation*) par ligne. Ici, une ligne du flux représente une transmission. Deux transmissions peuvent concerner un même sinistre.

Sur un poste neuf dont les exports sont absents, `make data PY=.venv/bin/python` génère le jeu du TP. Le générateur utilise la graine 42, une valeur de départ fixe pour reproduire le tirage. Si les six fichiers sont présents, vérifier leurs noms et leurs volumes suffit. La génération réécrit les fichiers de sortie. Les données restent sur le poste, y compris les données personnelles fictives. Pour expliquer un contrôle, partager un effectif suffit. [Démarrage du dépôt](../../README.md) · [Inventaire du jeu](../../data/raw/LISEZ-MOI.md)

## 3. Démarrer le service qui reçoit les déclarations

Kafka garde les messages publiés pour qu’un autre programme puisse les lire ensuite. Le service qui les reçoit, les conserve et les sert s’appelle le *broker*. Docker Compose utilise la configuration du dépôt pour le démarrer.

```bash
docker compose up -d kafka
docker compose ps kafka
```

La première commande démarre Kafka en arrière-plan. La deuxième indique l’état du service. Le statut attendu est `Up`. Ce contrôle répond à la question « Le service tourne-t-il ? ». Au L1, la description du canal puis le comptage en base vérifieront successivement que Kafka répond et que les messages ont été copiés. [Configuration locale](../../docker-compose.yml) · [Installation](../INSTALL.md)

## 4. Dessiner le trajet d’une déclaration et nommer ses destinataires

Prenons un dégât des eaux déclaré deux fois après une coupure. La mutuelle doit pouvoir retrouver les deux transmissions, compter un seul dossier et savoir qui a décidé de le revoir. En suivant cette histoire sur le schéma, chaque couche trouve sa raison d’être.

| Moment | Question à laquelle on répond | Outil, résultat et destinataire |
|---|---|---|
| Réception | Qu’avons-nous reçu ? | Kafka transporte les déclarations. DuckDB, la base locale, conserve leurs versions brutes en **bronze**, avec les quatre fichiers de référence importés par lot. |
| Préparation | Peut-on lire et comparer les déclarations ? | Au L2, l’outil de transformation dbt prépare la couche **silver**. Il y interprète les champs, repère les répétitions et marque les anomalies. |
| Comptage | Combien de dossiers valides y a-t-il ? | En **gold**, les analystes disposent d’une ligne par sinistre valide. Un rejet garde un motif pour permettre son examen. |
| Orientation | Que savait-on lors de la déclaration ? | Les lots suivants préparent les variables disponibles à cet instant. Le modèle donne un score. Le gestionnaire suit le circuit standard, les enquêteurs reçoivent les dossiers à revoir en priorité. |
| Décision et suivi | Qui a tranché et sur quelle base ? | Une personne habilitée décide et motive tout refus. L’équipe suit ensuite les scores, les délais et les écarts entre groupes. |

Le schéma de L0 décrit la **cible**. Au terme de L1, on a démontré la réception en bronze. Pour expliquer la différence, montrer où s’arrête la démonstration du jour sur le schéma. [Schéma d’architecture et parcours complet](../architecture.md)

## 5. Définir en dix lignes ce qu’est un système utile

La première mesure est la charge induite. L’équipe vise au plus 10 dossiers orientés sur 100 sinistres valides uniques et une première revue sous 48 heures à partir de l’entrée en file. Le volume de pointe et la façon de compter les heures restent à valider avec les enquêteurs. Une moyenne annuelle suffit rarement à prévoir la charge d’un lundi matin.

Deux autres mesures éclairent le choix du seuil. Imaginons que 9 dossiers orientés aient désormais une enquête clôturée, dont 3 fraudes confirmées. La **précision** observée est de 3 sur 9, soit une fraude confirmée sur trois dossiers orientés à conclusion connue. Si, sur le même périmètre d’évaluation, 5 fraudes sont connues au total et que le système en a orienté 3, le **rappel** vaut 3 sur 5, soit 60 %. Les dossiers sans conclusion restent inconnus. Le projet garde 70 % de rappel comme ambition à calibrer au L5, dans la limite de la capacité de revue.

Les dix critères couvrent aussi la comparaison des modèles, la décision humaine, la traçabilité et la fraîcheur, les explications, l’équité, la protection des données et la surveillance dans le temps. Chacun doit conduire à une observation concrète. Par exemple, pour la traçabilité, il faut pouvoir retrouver quelle version du modèle a produit un score. [Les dix critères et leurs hypothèses](../succes.md)

## Vérifier que L0 est prêt à être présenté

Les trois livrables demandés sont le schéma d’architecture, la définition du succès et le journal IA initialisé. Pour les présenter, suivre une déclaration jusqu’à un enquêteur, expliquer pourquoi la capacité des quatre enquêteurs limite le nombre d’alertes, puis montrer une décision réelle consignée au journal. La preuve technique qui vient ensuite consiste à faire voyager une déclaration jusqu’en bronze. [Cahier du TP, p. 3](../../supports/03_cahier_TP_fil_rouge.pdf) · [Grille d’évaluation](../../supports/04_grille_evaluation.pdf) · [Fiche L1](L1-ingestion.md)
