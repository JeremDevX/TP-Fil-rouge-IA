# L0 · Cadrer le projet et préparer le poste

La Mutuelle Alpine Assurances reçoit environ 4 000 déclarations de sinistre par an. Quatre enquêteurs doivent décider quels dossiers examiner en priorité. Imaginons que 100 déclarations valides arrivent cette semaine. Si le système en signale 60, qui pourra les examiner dans les délais ? Cette question de charge explique pourquoi le TP commence par définir le succès avant de construire un modèle.

À la fin de L0, l’équipe doit disposer d’un poste prêt pour l’ingestion, pouvoir suivre une déclaration sur un schéma et expliquer comment elle jugera le système utile. Les commandes ci-dessous se lancent depuis la racine du dépôt.

## 1. Former l’équipe et organiser les contributions

Le kit de départ fournit les fichiers communs du TP. Git conserve l’historique des modifications. La branche `main` représente la version partagée de l’équipe. Une branche de travail permet de préparer un changement, par exemple le chargement des déclarations, et de le tester avant son intégration.

Le parcours suit quatre gestes. Créer une branche pour le lot, enregistrer les modifications cohérentes dans un *commit*, ouvrir une demande de fusion (*pull request*) vers `main`, puis faire relire les changements par un autre membre. La protection de `main` fait appliquer les règles d’intégration sur GitHub. Pour la vérifier, consulter les règles de branche applicables à `main` et vérifier que la demande de fusion est exigée. La simple présence d’anciennes demandes de fusion prouve leur usage, mais ne suffit pas à établir que la règle est activée.

Le journal d’usage de l’intelligence artificielle (IA) commence dès ce cadrage. L’équipe y indique ses membres et consigne les usages significatifs, les propositions acceptées, corrigées ou refusées, les vérifications et le temps estimé. Par exemple, le journal du projet explique pourquoi la cible de rappel a été révisée. Cet exemple permet de raconter une décision en soutenance. Les estimations de temps et les appréciations restent à renseigner par leurs auteurs. [Journal d’usage](../USAGE-IA.TXT)

### Faire une première relecture en binôme

Imaginez qu’une personne ajoute le chargeur des contrats et qu’une autre doive reprendre son travail demain. La première explique dans sa demande de fusion ce qui entre, ce qui doit sortir et comment elle l’a testé. La seconde ouvre la différence entre les versions, choisit une affirmation précise (« un deuxième import conserve le même effectif »), puis essaie de la vérifier. Les rôles peuvent s’inverser au lot suivant.

Trois commandes permettent de s’orienter dans un dépôt déjà préparé. Elles se limitent à la lecture.

```bash
git status --short --branch
git log -1 --oneline
git diff --stat main...HEAD
```

La première ligne du statut donne le nom de la branche. Des lignes supplémentaires signalent des fichiers en cours de modification. La deuxième commande affiche le dernier commit de la branche. La troisième donne, fichier par fichier, l’ampleur des changements **déjà enregistrés** depuis la séparation avec la branche locale `main`. Un fichier tout juste modifié peut donc apparaître dans le statut et manquer dans ce dernier résumé. Pour le voir avant de le préparer pour un commit, utiliser `git diff`.

**Exercice de passation.** Montrez à votre binôme le fichier qui décrit le résultat attendu et celui qui en apporte la preuve. Pour le rejeu des référentiels, il peut lire la règle dans l’architecture puis retrouver le test dans le dossier `tests/`. Le journal IA complète ces deux traces en expliquant qui a vérifié la proposition et quelle correction en est sortie. [Test du chargement des référentiels](../../tests/test_load_referentiels.py)

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

### Compter les enregistrements sans ouvrir les dossiers individuels

Le nombre de fichiers répond à « ai-je reçu les six sources ? ». Le nombre d’enregistrements répond à « ai-je le jeu prévu pour cet exercice ? ». Un CSV contient une ligne d’en-tête avec les noms de colonnes. De plus, un champ entre guillemets peut contenir un retour à la ligne. Le lecteur CSV de Python sait où finit chaque enregistrement. Pour le JSONL du TP, une ligne du fichier correspond à un événement.

Ce bloc contrôle les six chemins attendus et affiche seulement leurs effectifs. Avec un fichier manquant, l’erreur indique le chemin à examiner.

```bash
.venv/bin/python - <<'PY'
import csv
from pathlib import Path

sources = (
    "assures.csv", "contrats.csv", "prestataires.csv", "enquetes.csv",
    "sinistres_events.jsonl", "sinistres_events_2026_01.jsonl",
)
for nom in sources:
    with (Path("data/raw") / nom).open(encoding="utf-8-sig", newline="") as fichier:
        if nom.endswith(".csv"):
            lignes = csv.reader(fichier)
            next(lignes)  # L’en-tête décrit les colonnes.
        else:
            lignes = fichier
        print(nom, sum(1 for _ in lignes))
PY
```

Contrôle effectué sur le poste du TP : les six comptages correspondent au tableau ci-dessus. Si vous trouvez **800** au lieu de **12 080** pour le flux que vous envisagez d’envoyer, vérifiez le nom du fichier avant de continuer. Janvier 2026 sert à observer plus tard comment le système réagit à une nouvelle période. Le charger ici changerait la référence de départ. Si Python signale qu’une bibliothèque manque au lancement d’un script du projet, vérifiez d’abord quel interpréteur vous utilisez avec `.venv/bin/python --version`. La procédure du dépôt prévoit Python 3.11.

## 3. Démarrer le service qui reçoit les déclarations

Kafka garde les messages publiés pour qu’un autre programme puisse les lire ensuite. Le service qui les reçoit, les conserve et les sert s’appelle le *broker*. Docker Compose utilise la configuration du dépôt pour le démarrer.

```bash
docker compose up -d kafka
docker compose ps kafka
```

La première commande démarre Kafka en arrière-plan. La deuxième indique l’état du service. Le statut attendu est `Up`. Ce contrôle répond à la question « Le service tourne-t-il ? ». Au L1, la description du canal puis le comptage en base vérifieront successivement que Kafka répond et que les messages ont été copiés. [Configuration locale](../../docker-compose.yml) · [Installation](../INSTALL.md)

### Distinguer conteneur démarré et service prêt

Un **conteneur** est l’environnement dans lequel tourne le programme Kafka. Ici, `docker compose` lit le fichier `docker-compose.yml` du projet. Le nom `kafka` sélectionne ce service et `-d` le laisse fonctionner quand la commande rend la main. On peut donc utiliser le même terminal pour interroger le serveur.

```bash
docker compose ps --all kafka
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Lisez les deux résultats dans l’ordre. Si le service est arrêté, la première commande peut afficher `Exited`. Si le service tourne, la seconde lance **dans le conteneur** un client qui demande au serveur la liste de ses canaux. Sur une installation neuve, une liste vide avec une commande terminée normalement est un résultat possible. Sur le poste du TP, le contrôle a affiché un service `Up` et le canal `maa.sinistres.declares.v1` dans la liste.

**Si le contrôle échoue.** Une erreur de connexion au moteur Docker dès la première commande invite à vérifier que Docker est démarré sur le poste. Si le conteneur vient de passer à `Up` et que le client ne peut pas encore joindre Kafka, laisser le service finir son démarrage puis relancer la commande de lecture. Ces deux symptômes interviennent à des endroits différents du parcours.

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

### Construire le schéma sur une feuille

1. À gauche, dessinez deux entrées. Écrivez « une transmission » sur la flèche des déclarations et « un export complet » sur celle des quatre fichiers. Cela explique dès le départ pourquoi un seul mode de collecte ne décrit pas tout le projet.
2. Au centre, placez bronze, silver et gold. Sous chaque boîte, écrivez ce que représente **une ligne**. C’est son *grain*. Pour le flux des sinistres, bronze conserve une transmission, tandis que la table principale de gold vise un sinistre valide unique. Deux transmissions du même dossier permettent de tester votre dessin.
3. À droite, placez les destinataires. Un dossier exploitable peut être orienté vers un gestionnaire ou vers les enquêteurs. Une déclaration invalide doit pouvoir être retrouvée avec une raison de rejet. Dessinez cette sortie avant la partie qui calcule le score.
4. Ajoutez la boucle de retour. Une enquête peut se terminer plusieurs semaines après la déclaration. Sa conclusion alimentera l’étude des dossiers passés lorsqu’elle sera connue. Utiliser cette conclusion pour calculer le score initial donnerait au modèle une information venue du futur.

**Test de lecture à deux.** Une personne pointe une flèche et demande « qui écrit quoi, et qui le relit ? ». Sur le trajet Kafka vers bronze, la réponse attendue est : le producteur a publié une déclaration, le consommateur lit son texte et sa position, puis l’enregistre dans DuckDB. Sur le trajet vers l’enquêteur, la réponse décrit une orientation à examiner. La décision d’indemnisation reste attribuée à la personne habilitée. Si la même flèche semble déjà prendre cette décision, précisez son libellé.

## 5. Définir en dix lignes ce qu’est un système utile

La première mesure est la charge induite. L’équipe vise au plus 10 dossiers orientés sur 100 sinistres valides uniques et une première revue sous 48 heures à partir de l’entrée en file. Le volume de pointe et la façon de compter les heures restent à valider avec les enquêteurs. Une moyenne annuelle suffit rarement à prévoir la charge d’un lundi matin.

Deux autres mesures éclairent le choix du seuil. Imaginons que 9 dossiers orientés aient désormais une enquête clôturée, dont 3 fraudes confirmées. La **précision** observée est de 3 sur 9, soit une fraude confirmée sur trois dossiers orientés à conclusion connue. Si, sur le même périmètre d’évaluation, 5 fraudes sont connues au total et que le système en a orienté 3, le **rappel** vaut 3 sur 5, soit 60 %. Les dossiers sans conclusion restent inconnus. Le projet garde 70 % de rappel comme ambition à calibrer au L5, dans la limite de la capacité de revue.

Les dix critères couvrent aussi la comparaison des modèles, la décision humaine, la traçabilité et la fraîcheur, les explications, l’équité, la protection des données et la surveillance dans le temps. Chacun doit conduire à une observation concrète. Par exemple, pour la traçabilité, il faut pouvoir retrouver quelle version du modèle a produit un score. [Les dix critères et leurs hypothèses](../succes.md)

### Passer des objectifs à des mesures vérifiables

Prenons une semaine fictive avec **100 sinistres valides uniques, 10 orientations, 9 enquêtes orientées clôturées et 3 fraudes confirmées parmi ces 9**. Sur le périmètre évalué, on connaît 5 fraudes en tout. Voici quatre calculs à refaire à la main avant de choisir un indicateur.

| Question | Calcul sur l’exemple | Ce que l’équipe peut en conclure |
|---|---|---|
| Combien de travail entrera en revue ? | 10 / 100 = 10 % | Le volume respecte ici le plafond proposé. Il faut encore examiner la répartition dans la semaine. |
| Quelle part des orientations a une conclusion ? | 9 / 10 = 90 % | Un dossier orienté est encore ouvert. Sa conclusion pourra modifier la précision mesurée. |
| Parmi les orientations clôturées, combien de fraudes ? | 3 / 9 ≈ 33 % | C’est la précision observée avec le recul disponible. |
| Parmi les fraudes connues, combien ont été orientées ? | 3 / 5 = 60 % | C’est le rappel observé. Le sort des dossiers jamais enquêtés reste inconnu. |

Pour évaluer le **délai**, les effectifs ne suffisent plus. Il faut l’heure d’entrée en file et celle de la première revue. De lundi 14 h à mercredi 10 h, il s’écoule 44 heures calendaires. Cette convention doit être précisée avant de comparer le résultat à l’objectif de 48 h.

**Comment rédiger une ligne du livrable ?** Partez d’une décision, puis écrivez la population concernée, la mesure et la preuve que vous chercherez. Pour la charge : « Sur la période retenue, diviser le nombre de sinistres valides uniques orientés par le nombre de sinistres valides uniques reçus. Comparer au plafond proposé de 10 %. » Pour l’équité, la preuve change : comparer les taux entre groupes avec leurs effectifs et leur part de conclusions connues. Pour la décision humaine, suivre un dossier jusqu’à la décision motivée. Une précision élevée, à elle seule, ne répond à aucune de ces deux dernières questions.

## Vérifier que L0 est prêt à être présenté

Les trois livrables demandés sont le schéma d’architecture, la définition du succès et le journal IA initialisé. Pour les présenter, suivre une déclaration jusqu’à un enquêteur, expliquer pourquoi la capacité des quatre enquêteurs limite le nombre d’alertes, puis montrer une décision réelle consignée au journal. La preuve technique qui vient ensuite consiste à faire voyager une déclaration jusqu’en bronze. [Cahier du TP, p. 3](../../supports/03_cahier_TP_fil_rouge.pdf) · [Grille d’évaluation](../../supports/04_grille_evaluation.pdf) · [Fiche L1](L1-ingestion.md)
