import json
import logging
from datetime import datetime, timedelta

import pandas as pd
import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- CONFIGURATION ---
POSTGRES_CONN_ID = "postgres_default"
S3_CONN_ID = "aws_minio_default"
DEFAULT_MINIO_BUCKET = "forexflow-raw"
API_URL = "https://api.frankfurter.app"

default_args = {
    "owner": "Antigravity",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="forex_flow_v1",
    default_args=default_args,
    description="Pipeline de suivi des taux de change Frankfurter",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 4, 1, tz="UTC"),
    catchup=True,
    tags=["forex", "etl", "taskflow", "minio"],
)
def forex_flow():
    @task()
    def init_database():
        """Initialise les tables et les vues si elles n'existent pas."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        with open("/opt/airflow/dags/sql/init_db.sql", "r") as f:
            hook.run(f.read(), split_statements=True)

        with open("/opt/airflow/dags/sql/analysis_queries.sql", "r") as f:
            hook.run(f.read(), split_statements=True)

        logging.info("Base de donnees et vues initialisees avec succes.")

    @task(retries=3, execution_timeout=timedelta(seconds=30))
    def extract_forex_data(ds=None, **kwargs):
        """Extrait les donnees de l'API Frankfurter et les stocke en brut."""
        base_curr = Variable.get("forex_base_currency", default_var="EUR")
        targets = Variable.get(
            "forex_target_currencies",
            default_var="USD,GBP,JPY,CHF,CAD",
        )

        url = f"{API_URL}/{ds}?from={base_curr}&to={targets}"

        logging.info("Interrogation de l'API: %s", url)
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = "INSERT INTO raw_forex (payload) VALUES (%s) RETURNING id"
        raw_id = hook.get_first(sql, (json.dumps(data),))[0]

        return {"raw_id": raw_id, "data": data}

    @task()
    def archive_raw_to_minio(extraction_result, ds=None, **kwargs):
        """Archive le JSON brut dans MinIO pour historisation hors base."""
        raw_id = extraction_result["raw_id"]
        data = extraction_result["data"]
        bucket_name = Variable.get(
            "forex_minio_bucket",
            default_var=DEFAULT_MINIO_BUCKET,
        )
        object_key = (
            f"raw/forex/year={ds[0:4]}/month={ds[5:7]}/day={ds[8:10]}/"
            f"forex_{ds}_raw_{raw_id}.json"
        )

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        if not s3_hook.check_for_bucket(bucket_name=bucket_name):
            s3_hook.create_bucket(bucket_name=bucket_name)

        s3_hook.load_string(
            string_data=json.dumps(data, ensure_ascii=False, indent=2),
            key=object_key,
            bucket_name=bucket_name,
            replace=True,
        )

        logging.info("Archive MinIO creee: s3://%s/%s", bucket_name, object_key)
        return {"bucket": bucket_name, "key": object_key}

    @task()
    def validate_and_transform(extraction_result, **kwargs):
        """Valide, transforme et charge les donnees dans la table clean."""
        raw_id = extraction_result["raw_id"]
        data = extraction_result["data"]

        stats = {"received": 0, "valid": 0, "rejected": 0, "inserted": 0}
        rejection_reason = None

        if not all(k in data for k in ["amount", "base", "date", "rates"]):
            rejection_reason = "Structure JSON invalide"
        elif len(data.get("rates", {})) < 5:
            rejection_reason = (
                f"Nombre de devises insuffisant: {len(data.get('rates', {}))}"
            )

        freshness_threshold = int(Variable.get("forex_freshness_threshold_days", 1))
        api_date = datetime.strptime(data["date"], "%Y-%m-%d").date()
        exec_date = datetime.strptime(kwargs["ds"], "%Y-%m-%d").date()

        if abs((exec_date - api_date).days) > freshness_threshold:
            rejection_reason = (
                f"Donnee trop ancienne (ecart > {freshness_threshold} jours)"
            )

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        if rejection_reason:
            logging.warning("Rejet de la donnee raw_id %s: %s", raw_id, rejection_reason)
            hook.run(
                "INSERT INTO rejects_forex (raw_id, reason, payload) VALUES (%s, %s, %s)",
                parameters=(raw_id, rejection_reason, json.dumps(data)),
            )
            stats["rejected"] = len(data.get("rates", {})) or 1
            return stats

        rates = data["rates"]
        stats["received"] = len(rates)
        df = pd.DataFrame(list(rates.items()), columns=["target_currency", "rate"])
        df["rate_date"] = data["date"]
        df["base_currency"] = data["base"]

        conn = hook.get_conn()
        cursor = conn.cursor()
        inserted_count = 0
        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO clean_forex (rate_date, base_currency, target_currency, rate)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (rate_date, base_currency, target_currency) DO NOTHING
                """,
                (
                    row["rate_date"],
                    row["base_currency"],
                    row["target_currency"],
                    row["rate"],
                ),
            )
            if cursor.rowcount > 0:
                inserted_count += 1
        conn.commit()
        cursor.close()
        conn.close()

        stats["valid"] = len(rates)
        stats["inserted"] = inserted_count
        return stats

    @task()
    def detect_alerts(extraction_result):
        """Detecte les variations anormales par rapport au dernier taux connu."""
        data = extraction_result["data"]
        base, date = data["base"], data["date"]
        threshold = float(Variable.get("forex_alert_threshold", 0.05))

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        alerts_count = 0

        for target, new_rate in data["rates"].items():
            sql = (
                "SELECT rate FROM clean_forex "
                "WHERE base_currency = %s AND target_currency = %s AND rate_date < %s "
                "ORDER BY rate_date DESC LIMIT 1"
            )
            prev_rate_row = hook.get_first(sql, (base, target, date))

            if prev_rate_row:
                old_rate = float(prev_rate_row[0])
                variation = abs((float(new_rate) - old_rate) / old_rate)
                if variation > threshold:
                    hook.run(
                        "INSERT INTO alerts_forex (currency_pair, old_rate, new_rate, variation_pct) "
                        "VALUES (%s, %s, %s, %s)",
                        parameters=(f"{base}/{target}", old_rate, new_rate, variation),
                    )
                    alerts_count += 1
        return alerts_count

    @task()
    def log_pipeline_metrics(stats, alerts_count, **kwargs):
        """Journalise les resultats du run."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run(
            """
            INSERT INTO logs_forex (run_id, status, lines_received, lines_valid, lines_rejected, lines_inserted, execution_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            parameters=(
                kwargs["run_id"],
                "SUCCESS",
                stats["received"],
                stats["valid"],
                stats["rejected"],
                stats["inserted"],
                kwargs["logical_date"],
            ),
        )

        logging.info("Alertes detectees pendant le run: %s", alerts_count)

    init = init_database()
    raw_data = extract_forex_data()
    archived_raw = archive_raw_to_minio(raw_data)
    metrics = validate_and_transform(raw_data)
    alerts = detect_alerts(raw_data)

    init >> raw_data
    raw_data >> archived_raw
    log_pipeline_metrics(metrics, alerts)


forex_dag = forex_flow()
