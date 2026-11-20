# Définition du succès — système d'aide à la détection de fraude (MAA)

Le système **réussit** s'il aide les 4 enquêteurs à mieux cibler leurs dossiers, sans jamais décider à leur place, sans discriminer, et en restant traçable.

1. **Taux de revue soutenable** : ≤ 10 % des déclarations orientées en `revue_renforcee` (≈ 400 dossiers/an pour ~4 000 déclarations, soit ~100 par enquêteur et par an), compatible avec une revue humaine sous 48 h. *Hypothèse de capacité à confirmer avec le métier.*
2. **Rappel** : au moins 70 % des fraudes confirmées du jeu de test se trouvent dans la file `revue_renforcee`.
3. **Précision** : au moins 1 dossier orienté sur 3 aboutit à une fraude confirmée, contre un ciblage « au flair » à mesurer comme référence (baseline).
4. **PR-AUC** strictement supérieure à la baseline (règles du guide interne seules), avec un gain mesuré et non une simple impression.
5. **Zéro décision automatique** : 100 % des refus d'indemnisation portent une décision humaine motivée et l'identité du décideur.
6. **Traçabilité** : 100 % des scores sont journalisés avec la version du modèle (MLflow), la version des données et un `trace_id`.
7. **Explicabilité** : chaque score en revue renforcée s'accompagne de ses 3 principaux facteurs, formulés en langage gestionnaire.
8. **Équité** : l'écart de taux de revue et de faux positifs entre tranches d'âge, zones et sexes reste ≤ 5 points, ou bien il est justifié et documenté. Aucun indicateur interdit (sexe, âge, lieu de résidence en tant que tels) n'est utilisé comme feature.
9. **Protection des données** : aucun identifiant direct (nom, email, IBAN, téléphone) dans les couches gold et ml ; tous les jeux de données sont catalogués avec leur base légale et leur durée de rétention.
10. **Robustesse** : la dérive (PSI > 0,2 sur une feature ou sur le score) déclenche une alerte, et le modèle reste dans ces seuils sur les données de janvier 2026 ou bien il est ré-entraîné selon la politique définie.

> Limite connue : le label n'existe que pour les dossiers **enquêtés**. Les métriques 2 à 4 mesurent donc la capacité du modèle à retrouver ce que les enquêteurs ont déjà trouvé, pas la fraude réelle (biais de sélection, à traiter dans le rapport d'éthique).
