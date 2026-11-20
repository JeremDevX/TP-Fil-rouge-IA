# L1 · Amener les données brutes jusqu’à DuckDB

À la fin de ce lot, on doit pouvoir répondre à deux questions simples. Combien de déclarations a-t-on reçues ? Si la lecture s’interrompt, peut-on la reprendre sans perdre une déclaration ni compter deux fois la même transmission ?

Le TP utilise deux chemins d’entrée. Les déclarations arrivent sous forme de messages dans Kafka. Les informations utiles pour les interpréter, comme les contrats ou les conclusions d’enquête, arrivent sous forme de fichiers complets. Les deux chemins alimentent **bronze**, la zone de réception brute dans la base locale `warehouse/maa.duckdb`. Les commandes suivantes se lancent depuis la racine du dépôt préparé au [L0](L0-cadrage-installation.md).

## 1. Préparer le canal Kafka et ses trois partitions

Un *topic* est un canal nommé où le producteur dépose ses messages. Ici, son nom est `maa.sinistres.declares.v1`. Kafka le divise en trois journaux ordonnés, appelés *partitions*. Chaque message a une position numérique, l’*offset*, dans sa partition. La position 7 de la partition 0 et la position 7 de la partition 1 désignent deux transmissions différentes.

Sur le Kafka local démarré au L0, ces commandes créent le canal s’il manque puis affichent sa configuration.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic maa.sinistres.declares.v1 --partitions 3 --replication-factor 1

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic maa.sinistres.declares.v1
```

Chercher `PartitionCount: 3` dans la description. La première commande laisse un canal existant en place, même si sa configuration diffère, d’où l’intérêt de la seconde. Le facteur de réplication 1 convient au TP qui dispose d’un seul serveur Kafka. Depuis ce conteneur les commandes utilisent le port 9092. Les scripts Python lancés sur le poste utilisent le port 19092. [Configuration Kafka](../../docker-compose.yml)

### Lire le résultat de la description du canal

Décomposez la commande. `docker compose exec kafka` choisit le conteneur où lancer l’outil. `--bootstrap-server` lui indique une première adresse de contact. `--topic` nomme le canal concerné. Enfin, `--create` demande une création et `--describe` demande une lecture. Cette dernière commande suffit sur le poste déjà préparé.

Dans la description, repérez les entrées `Partition: 0`, `Partition: 1` et `Partition: 2`. Le champ `Leader` désigne le serveur qui traite les demandes pour cette partition. Le même serveur peut apparaître trois fois : le TP répartit le journal en trois parties sur un seul serveur. Trois partitions permettent d’organiser la lecture ; trois copies des données sur des serveurs distincts répondraient à un autre besoin, celui de survivre à la perte d’un serveur.

**Exercice sur papier.** Placez deux messages A puis B dans la partition 0 aux positions 7 et 8, puis C dans la partition 1 à la position 7. Que sait-on de leur ordre ? A précède B dans la partition 0. La position 7 de C ne permet pas de situer C avant ou après A. Cette distinction explique pourquoi le producteur doit choisir une clé stable pour les messages d’un même sinistre.

## 2. Publier les déclarations et comprendre la clé

Le producteur lit le fichier de déclarations ligne par ligne. Pour chaque ligne, il extrait `id_sinistre` et l’utilise comme **clé de répartition**. Il envoie la ligne d’origine comme contenu du message. Avec le même nombre de partitions, les messages ayant le même identifiant arrivent dans la même partition. L’ordre d’écriture de cette partition permet donc de retrouver dans quel ordre ces transmissions sont arrivées. Kafka peut traiter d’autres sinistres dans les autres partitions.

Pour **le premier chargement d’un canal neuf**, le point d’entrée est le suivant.

```bash
.venv/bin/python ingestion/producer.py
```

Sur le fichier initial, le total attendu est de 12 080 événements publiés. Relancer ce programme republie le fichier et crée de nouvelles positions. C’est une autre expérience que la reprise de lecture demandée pour clore le lot. Sur le poste où L1 a déjà été chargé, passer directement au consommateur pour démontrer le rejeu. [Producteur fourni](../../ingestion/producer.py)

### Suivre une ligne dans le producteur

| Instruction à retrouver dans le script | Effet pour la déclaration |
|---|---|
| `for line in f` | Le programme parcourt le fichier initial une transmission à la fois. |
| `json.loads(line)` | Il lit les champs pour retrouver `id_sinistre`. Une ligne illisible comme objet JSON interrompt ce parcours avant l’envoi de cette ligne. |
| `key=evt["id_sinistre"]` et `value=line.encode("utf-8")` | La clé guide la répartition. Le contenu envoyé reste le texte de la ligne, converti en octets pour le transport. L’ordre et les espaces du texte reçu sont ainsi conservés. |
| `headers` | Le producteur joint deux indications séparées du contenu, la version du schéma et le nom du producteur. Le lecteur peut consulter dans le script les colonnes effectivement conservées en bronze. |
| `p.flush()` | Le programme attend le traitement des envois en attente tous les 1 000 messages puis en fin de fichier. Le compteur du producteur suit son envoi. Le comptage en bronze apporte une preuve indépendante de la copie en base. |

**Prédire avant d’exécuter.** Supposons que deux lignes aient la clé A et une troisième la clé B. Les deux A vont dans la même partition. B peut aussi se trouver dans cette partition : la règle regroupe les messages d’une même clé, plusieurs clés peuvent partager un groupe. Trois partitions n’impliquent donc ni trois sinistres ni trois effectifs égaux.

Le script active `enable.idempotence` côté producteur. Ce réglage couvre les tentatives de retransmission du client Kafka lors d’un envoi. Un nouveau lancement du script relit le fichier et demande volontairement de nouveaux envois. Pour examiner les paramètres disponibles sur un poste déjà chargé, `.venv/bin/python ingestion/producer.py --help` affiche l’aide puis quitte avant de publier. Cherchez notamment `--file` pour le fichier source et `--rate` pour le débit.

## 3. Copier les messages en bronze et reprendre après une interruption

Le **consommateur** lit les partitions et copie les messages dans `bronze.sinistres_events`. Il se fixe une borne de fin pour chaque partition, écrit les données par lots, puis confirme à Kafka la prochaine position à lire après avoir enregistré les messages de la partition. Les nouveaux messages au-delà de cette borne pourront être lus au lancement suivant.

| Champ bronze | Ce qu’il permet de retrouver |
|---|---|
| `payload` | Le texte original de la déclaration, y compris une valeur qui demandera une correction en L2. |
| `source` | Le canal Kafka d’origine ou la version du fichier utilisé en mode local. |
| `partition` et `offset` | L’adresse de cette transmission dans sa source. |
| `ingested_at` | Le moment de la copie en bronze, qui peut être postérieur à la déclaration. |

**Pourquoi confirmer après l’écriture ?** Imaginons que l’offset 17 soit lu puis que le poste s’arrête. Si le consommateur avait déjà confirmé la position 18 avant d’écrire, la déclaration 17 pourrait manquer dans DuckDB. Avec l’ordre retenu, une panne après l’écriture mais avant la confirmation peut faire relire la position 17. La table impose l’unicité de la combinaison `(source, partition, offset)`. Une deuxième insertion de cette même transmission est ignorée. C’est l’**idempotence** du chargement : répéter la lecture produit le même état en base.

**Contrôle de fin du TP.** Une fois le fichier initial publié, exécuter deux fois le consommateur seul et comparer le total qu’il affiche.

```bash
.venv/bin/python ingestion/consumer_to_lake.py
.venv/bin/python ingestion/consumer_to_lake.py
```

**Observation sur le poste du TP.** La table contenait **12 080 événements Kafka répartis sur trois partitions**. Une relance du consommateur a laissé **12 080 lignes**, soit zéro ajout. Le raccourci `make bronze` inclut aussi le producteur : il republierait les déclarations et fausserait cette démonstration.

**Plan B sans Kafka.** Sur une base d’exercice neuve, `make bronze-file PY=.venv/bin/python` charge les référentiels puis lit directement le fichier de déclarations. La table a les mêmes colonnes. Le numéro de ligne, à partir de zéro, joue le rôle de l’offset. La source contient le nom du fichier et une empreinte de son contenu. Utiliser ce mode comme alternative dès le début de l’exercice : l’exécuter sur la base déjà alimentée par Kafka ajouterait une deuxième provenance pour les mêmes déclarations. [Consommateur et mode fichier](../../ingestion/consumer_to_lake.py) · [Commandes du projet](../../Makefile)

### Suivre la reprise sur trois messages

Dans `copy_partition`, le programme demande à Kafka la première position encore disponible et la borne de fin. Il regarde aussi la prochaine position confirmée pour le groupe `maa-bronze` et la dernière position présente dans DuckDB. Ces deux mémoires peuvent diverger après une interruption. Le programme repart assez tôt pour recopier ce qui manque dans la base, dans la limite de ce que Kafka conserve encore.

Supposons qu’une partition contienne les positions **17, 18 et 19** à lire. Sa borne de fin est **20**, la prochaine position libre. Suivez ce qui se passerait selon le moment de l’arrêt.

| Moment de l’arrêt | État possible de DuckDB | Effet de la relance |
|---|---|---|
| Avant la première écriture | Aucun des trois messages enregistré | Le consommateur peut encore lire 17, 18 et 19 et les enregistrer. |
| Après l’écriture, avant la confirmation à Kafka | Les trois positions déjà présentes, position confirmée restée à 17 | La relecture retrouve la même clé technique pour chaque transmission. Les trois insertions en conflit sont ignorées. La position 20 peut ensuite être confirmée. |
| Après la confirmation de 20 | Les trois positions présentes et la prochaine lecture fixée à 20 | Si la borne de fin est toujours 20, il n’y a rien à copier pour cette partition. |

C’est pourquoi le test de panne du dépôt fait échouer la **confirmation** après une écriture réussie. Une exécution normale ne passerait jamais par ce moment délicat. Le script regroupe jusqu’à 1 000 messages pour une lecture, écrit chaque lot avec une instruction de base de données, puis confirme la partition traitée jusqu’à sa borne. La taille du lot et la fréquence des confirmations répondent donc à deux questions distinctes.

### Vérifier les partitions directement en base

Cette commande ouvre DuckDB en lecture seule et compte les transmissions du canal du TP par partition. Elle n’affiche aucun contenu de déclaration.

```bash
.venv/bin/python - <<'PY'
import duckdb

with duckdb.connect("warehouse/maa.duckdb", read_only=True) as db:
    resultat = db.execute("""
        SELECT partition, count(*) AS transmissions
        FROM bronze.sinistres_events
        WHERE source = 'kafka:maa.sinistres.declares.v1'
        GROUP BY partition ORDER BY partition
    """).fetchall()
    for partition, transmissions in resultat:
        print(partition, transmissions)
    print("Total Kafka", sum(nombre for _, nombre in resultat))
PY
```

Trois lignes de détail indiquent que les trois partitions ont contribué. Sur le poste du TP, les partitions 0, 1 et 2 contenaient respectivement **3 927, 4 076 et 4 077** transmissions. Leur somme donne bien les **12 080** événements attendus. Sur une base alimentée uniquement par fichier, cette requête donne un total Kafka de zéro : son filtre sélectionne explicitement la provenance Kafka. Une comparaison avant et après la relance permet de localiser une hausse éventuelle par partition.

## 4. Charger ensemble les quatre référentiels

Un référentiel apporte du contexte à la déclaration. Par exemple, un dossier contient un identifiant de contrat. Le fichier des contrats permettra ensuite de retrouver le produit couvert. Pour l’instant, on conserve les champs en texte. Cela préserve les zéros de début d’un code postal et distingue la valeur reçue des conversions que l’on décidera en L2.

```bash
.venv/bin/python ingestion/load_referentiels.py
```

| Table créée dans DuckDB | Ce qu’elle reçoit | Premier import observé | Deuxième import observé |
|---|---|---:|---:|
| `bronze.assures` | Export des assurés | 4 000 | 4 000 |
| `bronze.contrats` | Export des contrats | 4 909 | 4 909 |
| `bronze.prestataires` | Export des prestataires | 120 | 120 |
| `bronze.enquetes` | Export des enquêtes | 2 841 | 2 841 |

Le script vérifie l’en-tête et le nombre de champs de chaque ligne. Il calcule aussi une empreinte du contenu du fichier, stockée dans `_source_sha256`. Cela permet de reconnaître un export déjà chargé. Si une ligne change, tout le fichier modifié constitue une nouvelle version. Pour une future jointure, on devra donc choisir une version par table pour éviter de multiplier les dossiers. Deux lignes identiques présentes dans un même fichier sont conservées : elles font partie de ce qui a été reçu.

Les quatre imports sont regroupés dans une **transaction**, un groupe d’écritures validées ensemble. Si le dernier fichier est mal formé, les ajouts des trois premiers sont annulés. Corriger le fichier puis relancer le lot permet de reprendre sur une base cohérente. Le test du dépôt reproduit ce cas sur des données de test sans afficher de ligne confidentielle. [Chargeur des référentiels](../../ingestion/load_referentiels.py) · [Test de reprise du lot](../../tests/test_load_referentiels.py)

### Refaire le raisonnement de l’import sur un petit lot

Imaginez un export de contrats de deux lignes. On le charge le lundi. Le mardi, l’export a toujours deux lignes, mais la formule d’un contrat a changé. Comparer seulement la taille du fichier ou son nombre de lignes ne suffit pas pour décider que le contenu est identique.

| Situation fictive | Décision du chargeur | Lignes de contrats conservées en bronze |
|---|---|---:|
| Lundi, premier fichier de deux contrats | Empreinte nouvelle, import complet | 2 |
| Lundi, relance avec exactement le même fichier | Empreinte déjà présente, aucun ajout | 2 |
| Mardi, une formule modifiée, tous les fichiers valides | Nouvelle empreinte, deuxième version complète | 4 |
| Nouvelle relance avec les fichiers du mardi | Version déjà présente, aucun ajout | 4 |

Les quatre lignes du mardi représentent deux **versions** de deux contrats. Pour compter les contrats d’un export, il faudra choisir l’empreinte correspondant à cet export. L’empreinte permet de reconnaître un contenu ; sa valeur ne dit pas à elle seule quel fichier est le plus récent.

**Et si le dernier CSV est invalide ?** Reprenez le lundi avec deux lignes en base. Mardi, le chargeur prépare les deux lignes du nouvel export de contrats, puis découvre un champ manquant dans le fichier des enquêtes. La transaction annule aussi l’ajout des contrats. On reste à **deux** lignes. Une fois le fichier corrigé, le même import peut être repris depuis le début et le total passe à **quatre**. Ici, « brut » autorise une forme métier inhabituelle, comme une date en texte. Le lecteur a tout de même besoin du bon nombre de colonnes pour savoir dans quel champ placer chaque valeur.

**Vérification reproductible.** Lancez `.venv/bin/python -m pytest -q tests/test_load_referentiels.py`. Le test construit des fichiers temporaires, conserve un identifiant commençant par des zéros et un champ contenant une virgule, puis essaie un dernier CSV incomplet. Le résultat attendu est `1 passed`. Il vérifie à la fois ce qui est conservé et ce qui doit être annulé, sans modifier les exports du projet.

## Répondre aux trois questions de réflexion

**a. Que change une livraison « au moins une fois » pour la suite ?** Une déclaration peut être présentée plusieurs fois. Si le consommateur relit la même position Kafka, bronze la reconnaît et conserve une ligne. Si le producteur republie la déclaration, Kafka lui attribue une autre position et bronze garde les deux transmissions. En L2, il faudra reconnaître qu’elles désignent le même sinistre avant de calculer le nombre de dossiers. Sur le jeu initial, 12 080 est un nombre d’événements reçus. Le cahier attend ensuite 11 883 sinistres valides uniques et 117 rejets ; ce sont des résultats à vérifier au L2.

**b. Import quotidien ou capture des changements à la source ?** Avec quatre exports complets et un besoin compatible avec une journée de décalage, un import par lot (*batch*) suffit. La capture des changements (*Change Data Capture*, CDC), par exemple avec Debezium, lirait les modifications du système source au fil de l’eau. Elle réduirait le délai, mais demanderait l’accès au journal de la base source et un connecteur à exploiter. Si une conclusion d’enquête devait être prise en compte en quelques minutes, cette exigence de fraîcheur justifierait de réexaminer le choix.

**c. Pourquoi garder les valeurs brutes ?** Imaginons un montant reçu sous la forme `"1234,56"`. Si une première règle interprète mal la virgule, la copie brute permet de corriger la règle puis de recalculer sur la valeur d’origine. C’est le **rejeu**. On peut aussi expliquer après coup ce qui était arrivé et pourquoi la déclaration avait été rejetée. Le nettoyage et les règles métier appartiennent à L2.

## Préparer la démonstration

Sur le poste déjà chargé, montrer les trois partitions, relancer uniquement le consommateur puis le chargeur de référentiels et comparer les effectifs ci-dessus. Le contrôle se fait avec des totaux : il n’y a aucune raison d’afficher des lignes d’assurés ou d’enquêtes.

Pour tester la panne entre écriture et confirmation sans arrêter le service, lancer `.venv/bin/python -m pytest -q tests/test_consumer_to_lake.py tests/test_load_referentiels.py`. Trois tests ont réussi sur la version L1 : rejeu du fichier de déclarations, échec simulé de confirmation Kafka après écriture, et reprise du lot de référentiels. Le test de panne simule Kafka, tandis que le contrôle des 12 080 lignes a été effectué avec le service local. Cette distinction permet d’expliquer précisément les preuves en soutenance. [Tests du consommateur](../../tests/test_consumer_to_lake.py) · [Cahier du TP, p. 3–4](../../supports/03_cahier_TP_fil_rouge.pdf) · [Grille d’évaluation](../../supports/04_grille_evaluation.pdf)
