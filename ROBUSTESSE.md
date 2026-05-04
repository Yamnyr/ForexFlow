# Architecture et Robustesse du Pipeline ForexFlow

Ce document détaille les principes de robustesse, d'idempotence et les différents niveaux de contrôles qualité mis en œuvre dans le pipeline ETL ForexFlow. L'objectif est de garantir la fiabilité et la traçabilité des données de taux de change.

## 1. Choix de Robustesse et Idempotence

Le pipeline a été conçu pour résister aux pannes et permettre des ré-exécutions sans risque de corruption ou de duplication des données.

- Idempotence de l'Extraction : Les appels à l'API Frankfurter sont basés sur la date logique d'exécution d'Airflow. Relancer le pipeline pour une date passée ramènera toujours les données exactes de cette même date.
- Insertion Sécurisée : L'intégration dans la base de données PostgreSQL (table clean_forex) utilise une contrainte d'unicité sur la combinaison (date, devise de base, devise cible) couplée à la commande ON CONFLICT DO NOTHING. Cela empêche toute duplication en cas de relance accidentelle ou de rattrapage (catchup).
- Archivage Brut (Data Lake) : Avant toute transformation, la réponse JSON originale est sauvegardée dans MinIO. En cas d'erreur dans la logique de transformation, il est possible de rejouer le pipeline sans avoir besoin de solliciter à nouveau l'API source.
- Atomicité des Tâches : Le DAG Airflow est découpé en tâches simples et indépendantes (extraction, archivage, validation, génération d'alertes). L'échec d'une étape limite l'impact sur le reste du flux.

## 2. Contrôles Qualité (Data Quality)

Avant l'insertion dans la table finale exploitable, les données brutes traversent trois filtres stricts. Si une donnée échoue à un contrôle, elle est écartée du flux principal.

- Validation Structurelle : Le système vérifie que le flux JSON contient bien les clés fondamentales attendues. Une structure altérée déclenche le rejet immédiat de la donnée.
- Contrôle d'Exhaustivité : Le pipeline exige un nombre minimum de devises cibles présentes dans la réponse. Si ce seuil n'est pas atteint, la donnée est considérée comme incomplète et non représentative.
- Contrôle de Fraîcheur : Un écart maximal (configurable) est toléré entre la date réelle des données fournies par l'API et la date d'exécution attendue du pipeline. Cela prévient l'intégration et l'affichage de données périmées.

## 3. Gestion des Rejets et Traçabilité

La philosophie du pipeline est de ne jamais détruire silencieusement l'information.

Les données ne passant pas les contrôles qualité sont redirigées vers une table de quarantaine (rejects_forex). Chaque rejet y est enregistré avec sa charge utile originale complète et le motif explicite de l'erreur, ce qui facilite le débogage.

De plus, un audit de chaque exécution est sauvegardé (table logs_forex), recensant le statut final, ainsi que le nombre exact de lignes reçues, validées, insérées et rejetées. Cela permet un suivi proactif et chiffré de la santé du pipeline.
