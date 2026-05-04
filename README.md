# Projet ForexFlow : Pipeline de Suivi des Taux de Change

Ce projet implemente un pipeline de donnees avec **Apache Airflow**, **PostgreSQL**, **Pandas** et **MinIO** pour suivre les taux de change via l'API Frankfurter.

## Installation et Configuration

### 1. Variables Airflow
Pour un fonctionnement optimal, configurez les variables suivantes dans l'interface Airflow (`Admin -> Variables`) :

| Cle | Valeur par defaut | Description |
| :--- | :--- | :--- |
| `forex_base_currency` | `EUR` | Devise de reference. |
| `forex_target_currencies` | `USD,GBP,JPY,CHF,CAD` | Liste des devises a suivre (minimum 5). |
| `forex_alert_threshold` | `0.05` | Seuil de variation, par exemple `0.05 = 5%`. |
| `forex_freshness_threshold_days` | `1` | Nombre maximum de jours d'ecart accepte entre la date API et la date d'execution. |
| `forex_minio_bucket` | `forexflow-raw` | Bucket MinIO cible pour l'archivage des donnees brutes. |

### 2. Connexion PostgreSQL
La connexion `postgres_default` est creee automatiquement via le fichier `docker-compose.yaml`.

### 3. Configuration MinIO
Avant de lancer la stack, verifiez les variables MinIO dans le fichier `.env`.
Pour un usage local, vous pouvez conserver les valeurs par defaut.

| Cle | Valeur par defaut | Description |
| :--- | :--- | :--- |
| `MINIO_ROOT_USER` | `minioadmin` | Identifiant administrateur MinIO. |
| `MINIO_ROOT_PASSWORD` | `minioadmin` | Mot de passe administrateur MinIO. |
| `MINIO_BUCKET_NAME` | `forexflow-raw` | Bucket cree automatiquement au demarrage. |
| `MINIO_REGION` | `us-east-1` | Region S3 utilisee par la connexion Airflow. |

Services exposes :

| Service | URL | Usage |
| :--- | :--- | :--- |
| API S3 | `http://localhost:9000` | Endpoint pour Airflow, scripts Python, SDK AWS S3 ou `boto3`. |
| Console Web | `http://localhost:9001` | Administration des buckets et objets. |
| Interface Airflow | `http://localhost:8080` | Interface de pilotage du pipeline. |

Lancement :

```bash
docker compose up -d
```

Le bucket `forexflow-raw` est cree automatiquement au demarrage, et la connexion Airflow `aws_minio_default` est injectee via les variables d'environnement Docker.

## Choix Techniques

### Idempotence
- Le pipeline utilise la date logique du run (`ds`) pour interroger l'API.
- La table `clean_forex` possede une contrainte `UNIQUE (rate_date, base_currency, target_currency)`.
- L'utilisation de `ON CONFLICT DO NOTHING` evite les doublons lors des relances.

### Data Quality
Le pipeline implemente 3 niveaux de validation :
1. Verification de la structure JSON recue.
2. Verification de la completude avec un minimum de 5 devises.
3. Verification de la fraicheur des donnees par rapport a la date d'execution.

### Gestion des erreurs
- Les donnees invalides sont stockees dans `rejects_forex` avec la raison du rejet.
- Les taches sont decoupees en etapes atomiques : extraction, validation, alerting, archivage.

### Archivage
- Les donnees brutes sont stockees en base dans `raw_forex`.
- Une copie JSON est archivee dans MinIO sous `raw/forex/year=YYYY/month=MM/day=DD/...`.

## Exploitation des Donnees

Utilisez les vues SQL creees pour vos analyses :
- `view_forex_evolution` : historique complet avec calcul des variations quotidiennes.
- `view_forex_volatility` : statistiques de volatilite sur les 30 derniers jours.
- `view_pipeline_health` : rapport de monitoring du pipeline.

## Structure du Projet

```text
ForexFlow/
|-- dags/
|   |-- forex_flow.py
|   `-- sql/
|       |-- init_db.sql
|       `-- analysis_queries.sql
|-- config/
|-- plugins/
|-- docker-compose.yaml
|-- .env
`-- README.md
```

## Exemples de Resultats

### Donnees nettoyees (`clean_forex`)

```text
 id | rate_date  | base_currency | target_currency |    rate    |         processed_at
----+------------+---------------+-----------------+------------+-------------------------------
  1 | 2024-04-08 | EUR           | CAD             |   1.471500 | 2026-04-30 08:44:04.962189+00
  2 | 2024-04-08 | EUR           | CHF             |   0.980700 | 2026-04-30 08:44:04.962189+00
  3 | 2024-04-08 | EUR           | GBP             |   0.857950 | 2026-04-30 08:44:04.962189+00
  4 | 2024-04-08 | EUR           | JPY             | 164.430000 | 2026-04-30 08:44:04.962189+00
  5 | 2024-04-08 | EUR           | USD             |   1.082300 | 2026-04-30 08:44:04.962189+00
```

### Alertes detectees (`alerts_forex`)

```text
 id | currency_pair |  old_rate  |  new_rate  | variation_pct |        alert_timestamp
----+---------------+------------+------------+---------------+-------------------------------
  1 | EUR/CAD       |   1.464500 |   1.600700 |        0.0930 | 2026-04-30 08:54:16.7343+00
  2 | EUR/CHF       |   0.972500 |   0.923600 |        0.0503 | 2026-04-30 08:54:16.758701+00
  3 | EUR/JPY       | 164.050000 | 187.050000 |        0.1402 | 2026-04-30 08:54:16.779723+00
  4 | EUR/USD       |   1.065600 |   1.170600 |        0.0985 | 2026-04-30 08:54:16.79505+00
```
