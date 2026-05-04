import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from datetime import datetime, timedelta
import os

# Configuration de la page
st.set_page_config(
    page_title="ForexFlow Dashboard",
    page_icon="📈",
    layout="wide"
)

# Fonction de connexion à la base de données
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "airflow"),
        user=os.getenv("DB_USER", "airflow"),
        password=os.getenv("DB_PASSWORD", "airflow"),
        port=os.getenv("DB_PORT", "5432")
    )

# Titre principal
st.title("📈 ForexFlow : Dashboard des Taux de Change")
st.markdown("---")

try:
    conn = get_connection()

    # --- SIDEBAR : Filtres ---
    st.sidebar.header("Configuration")
    
    # Récupérer la liste des devises
    df_currencies = pd.read_sql("SELECT DISTINCT target_currency FROM clean_forex", conn)
    currencies = df_currencies['target_currency'].tolist()
    
    selected_currency = st.sidebar.selectbox("Sélectionnez une devise", currencies if currencies else ["USD"])
    days_to_show = st.sidebar.slider("Nombre de jours", 7, 90, 30)

    # --- SECTION 1 : Indicateurs Clés (KPIs) ---
    col1, col2, col3, col4 = st.columns(4)

    # Dernier taux
    query_last = f"""
        SELECT rate, rate_date FROM clean_forex 
        WHERE target_currency = '{selected_currency}' 
        ORDER BY rate_date DESC LIMIT 1
    """
    df_last = pd.read_sql(query_last, conn)
    
    if not df_last.empty:
        last_rate = df_last['rate'].iloc[0]
        last_date = df_last['rate_date'].iloc[0]
        
        # Taux précédent pour le delta
        query_prev = f"""
            SELECT rate FROM clean_forex 
            WHERE target_currency = '{selected_currency}' 
            AND rate_date < '{last_date}'
            ORDER BY rate_date DESC LIMIT 1
        """
        df_prev = pd.read_sql(query_prev, conn)
        delta = None
        if not df_prev.empty:
            prev_rate = df_prev['rate'].iloc[0]
            delta = float((last_rate - prev_rate) / prev_rate)

        col1.metric(f"Taux {selected_currency}", f"{last_rate:.4f}", f"{delta:.2%}" if delta is not None else None)
        col2.metric("Dernière mise à jour", last_date.strftime('%d/%m/%Y'))
    
    # Nombre d'alertes 24h
    query_alerts = "SELECT COUNT(*) FROM alerts_forex WHERE alert_timestamp > NOW() - INTERVAL '24 hours'"
    alerts_count = pd.read_sql(query_alerts, conn).iloc[0, 0]
    col3.metric("Alertes (24h)", alerts_count, delta_color="inverse" if alerts_count > 0 else "normal")
    
    # Santé du pipeline
    query_health = "SELECT success_rate_pct FROM view_pipeline_health ORDER BY run_day DESC LIMIT 1"
    health = pd.read_sql(query_health, conn)
    if not health.empty and health.iloc[0, 0] is not None:
        col4.metric("Santé Pipeline", f"{health.iloc[0, 0]}%")
    else:
        col4.metric("Santé Pipeline", "0%", help="En attente de logs d'exécution")

    # --- SECTION 2 : Graphiques ---
    st.markdown("### Évolution des cours")
    
    query_evo = f"""
        SELECT rate_date, rate FROM clean_forex 
        WHERE target_currency = '{selected_currency}'
        AND rate_date > CURRENT_DATE - INTERVAL '{days_to_show} days'
        ORDER BY rate_date ASC
    """
    df_evo = pd.read_sql(query_evo, conn)
    
    if not df_evo.empty:
        fig = px.line(df_evo, x='rate_date', y='rate', 
                     title=f"Évolution de l'EUR/{selected_currency} (Derniers {days_to_show} jours)",
                     labels={'rate_date': 'Date', 'rate': 'Taux'},
                     template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Pas assez de données pour afficher le graphique.")

    # --- SECTION 3 : Alertes et Anomalies ---
    st.markdown("---")
    c1, c2 = st.columns(2)
    
    with c1:
        st.markdown("### 🚨 Dernières Alertes")
        df_alerts = pd.read_sql("SELECT currency_pair, old_rate, new_rate, variation_pct, alert_timestamp FROM alerts_forex ORDER BY alert_timestamp DESC LIMIT 5", conn)
        if not df_alerts.empty:
            st.dataframe(df_alerts, use_container_width=True)
        else:
            st.success("Aucune alerte récente détectée.")

    with c2:
        st.markdown("### 📊 Volatilité Relative (%)")
        df_vol = pd.read_sql("SELECT target_currency, volatility_rel_pct FROM view_forex_volatility", conn)
        if not df_vol.empty:
            fig_vol = px.bar(df_vol, x='target_currency', y='volatility_rel_pct', 
                            title="Volatilité normalisée (comparaison équitable)",
                            labels={'volatility_rel_pct': 'Volatilité (%)', 'target_currency': 'Devise'},
                            template="plotly_dark")
            st.plotly_chart(fig_vol, use_container_width=True)

    conn.close()

except Exception as e:
    st.error(f"Erreur de connexion à la base de données : {e}")
    st.info("Assurez-vous que les conteneurs Docker sont lancés et que PostgreSQL est accessible.")
