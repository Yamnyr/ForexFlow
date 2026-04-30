# Projet ForexFlow : Pipeline de Suivi des Taux de Change

Ce projet implémente un pipeline de données robuste avec **Apache Airflow**, **PostgreSQL** et **Pandas** pour suivre les taux de change via l'API Frankfurter.

## Installation & Configuration

### 1. Variables Airflow
Pour un fonctionnement optimal, configurez les variables suivantes dans l'interface Airflow (Admin -> Variables) :

| Clé | Valeur par défaut | Description |
| :--- | :--- | :--- |
| `forex_base_currency` | `EUR` | Devise de référence. |
| `forex_target_currencies` | `USD,GBP,JPY,CHF,CAD` | Liste des devises à suivre (min. 5). |
| `forex_alert_threshold` | `0.05` | Seuil de variation (ex: 0.05 = 5%). |
| `forex_freshness_threshold_days` | `1` | Nombre de jours max d'écart accepté entre la date de l'API et l'exécution. |

### 2. Connexion PostgreSQL
La connexion `postgres_default` est automatiquement créée via le fichier `docker-compose.yaml`.

---

## Explication des Choix Techniques (Data Engineering)

### Idempotence (Relançable à l'infini)
- **Date-based extraction** : Le pipeline utilise la date logique du run (`ds`) pour interroger l'API. Relancer le pipeline pour la même date produira le même résultat.
- **Contrainte d'unicité** : La table `clean_forex` possède une contrainte `UNIQUE (rate_date, base_currency, target_currency)`. L'utilisation de `ON CONFLICT DO NOTHING` garantit qu'aucun doublon n'est inséré lors des re-runs, évitant la corruption des données historiques.

### Data Quality (Contrôle Qualité)
Le pipeline implémente 3 niveaux de validation :
1. **Structure** : Vérification de la conformité du JSON reçu.
2. **Complétude** : On rejette la donnée si le nombre de devises est insuffisant (min 5).
3. **Fraîcheur** : Comparaison entre la date de la donnée API et la date d'exécution. Si l'API renvoie des données trop vieilles, elles sont rejetées.

### Gestion des Erreurs (Cimetière de données)
- **Table Rejects** : Les données invalides ne bloquent pas le pipeline mais sont isolées dans `rejects_forex` avec la raison du rejet pour audit.
- **Isolation des tâches** : Chaque étape est atomique (Extraction -> Validation -> Alerting).

### Robustesse
- **Retries & Timeouts** : L'extraction API possède 3 tentatives avec un délai de 5 minutes pour gérer les micro-coupures réseau.

---

## Exploitation des Données

Utilisez les vues SQL créées pour vos analyses :
- `view_forex_evolution` : Historique complet avec calcul des variations quotidiennes.
- `view_forex_volatility` : Statistiques de volatilité sur les 30 derniers jours.
- `view_pipeline_health` : Rapport de monitoring (taux de succès, rejets, etc.).

---

## Structure du Projet
```text
ForexFlow/
├── dags/
│   ├── forex_flow.py      # DAG Airflow (TaskFlow API)
│   └── sql/
│       ├── init_db.sql    # DDL des tables
│       └── analysis_queries.sql # Vues métier
└── README.md              # Documentation
```

---

## Exemples de Résultats

### Données nettoyées (clean_forex)
Voici un extrait des taux récupérés et structurés :
```text
 id | rate_date  | base_currency | target_currency |    rate    |         processed_at          
----+------------+---------------+-----------------+------------+-------------------------------
  1 | 2024-04-08 | EUR           | CAD             |   1.471500 | 2026-04-30 08:44:04.962189+00
  2 | 2024-04-08 | EUR           | CHF             |   0.980700 | 2026-04-30 08:44:04.962189+00
  3 | 2024-04-08 | EUR           | GBP             |   0.857950 | 2026-04-30 08:44:04.962189+00
  4 | 2024-04-08 | EUR           | JPY             | 164.430000 | 2026-04-30 08:44:04.962189+00
  5 | 2024-04-08 | EUR           | USD             |   1.082300 | 2026-04-30 08:44:04.962189+00
```

### Alertes détectées (alerts_forex)
Exemple d'alertes générées lors de variations supérieures au seuil :
```text
 id | currency_pair |  old_rate  |  new_rate  | variation_pct |        alert_timestamp        
----+---------------+------------+------------+---------------+-------------------------------
  1 | EUR/CAD       |   1.464500 |   1.600700 |        0.0930 | 2026-04-30 08:54:16.7343+00
  2 | EUR/CHF       |   0.972500 |   0.923600 |        0.0503 | 2026-04-30 08:54:16.758701+00
  3 | EUR/JPY       | 164.050000 | 187.050000 |        0.1402 | 2026-04-30 08:54:16.779723+00
  4 | EUR/USD       |   1.065600 |   1.170600 |        0.0985 | 2026-04-30 08:54:16.79505+00
```
