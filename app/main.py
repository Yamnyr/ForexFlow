import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os

# --- CONFIGURATION ---
st.set_page_config(
    page_title="ForexFlow | Data Dashboard",
    page_icon="💹",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Style CSS personnalisé
st.markdown("""
    <style>
    .main {
        background-color: #0e1117;
    }
    .stMetric {
        background-color: #1e2130;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #3e4259;
    }
    div[data-testid="stExpander"] {
        border: none;
        box-shadow: none;
    }
    </style>
    """, unsafe_allow_html=True)

# --- DATABASE CONNECTION ---
@st.cache_resource
def get_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME", "airflow"),
            user=os.getenv("DB_USER", "airflow"),
            password=os.getenv("DB_PASSWORD", "airflow"),
            port=os.getenv("DB_PORT", "5432")
        )
        return conn
    except Exception as e:
        st.error(f"Impossible de se connecter à PostgreSQL : {e}")
        return None

@st.cache_data(ttl=300)
def load_data(query):
    conn = get_connection()
    if conn:
        return pd.read_sql(query, conn)
    return pd.DataFrame()

# --- HEADER ---
st.title("💹 ForexFlow : Monitoring & Intelligence")
st.caption("Pipeline de données automatisé pour le suivi des marchés des changes")

# --- SIDEBAR ---
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/2454/2454282.png", width=100)
    st.header("Paramètres")
    
    # Charger les devises disponibles
    df_currencies = load_data("SELECT DISTINCT target_currency FROM clean_forex ORDER BY 1")
    all_currencies = df_currencies['target_currency'].tolist() if not df_currencies.empty else ["USD", "GBP", "JPY", "CHF", "CAD"]
    
    selected_currencies = st.multiselect(
        "Devises à analyser", 
        options=all_currencies,
        default=["USD", "GBP"] if len(all_currencies) >= 2 else [all_currencies[0]]
    )
    
    lookback_days = st.slider("Fenêtre temporelle (jours)", 7, 365, 30)
    
    st.divider()
    st.info("💡 Les données sont rafraîchies toutes les 24h par le pipeline Airflow.")

# --- MAIN INTERFACE ---
if not selected_currencies:
    st.warning("Veuillez sélectionner au moins une devise dans la barre latérale.")
else:
    tab_market, tab_pipeline, tab_audit = st.tabs([
        "📈 Analyses Marchés", 
        "⚙️ Observabilité Pipeline", 
        "🔍 Audit & Raw Data"
    ])

    # --- TAB 1: ANALYSES MARCHÉS ---
    with tab_market:
        # 1. KPIs de tête
        cols = st.columns(len(selected_currencies) if len(selected_currencies) <= 4 else 4)
        for i, curr in enumerate(selected_currencies[:4]):
            with cols[i]:
                # Dernier taux et delta
                query = f"""
                    SELECT rate, rate_date 
                    FROM view_forex_evolution 
                    WHERE target_currency = '{curr}' 
                    ORDER BY rate_date DESC LIMIT 2
                """
                df_kpi = load_data(query)
                if len(df_kpi) >= 2:
                    current_rate = df_kpi.iloc[0]['rate']
                    prev_rate = df_kpi.iloc[1]['rate']
                    delta = (current_rate - prev_rate) / prev_rate
                    st.metric(label=f"EUR/{curr}", value=f"{current_rate:.4f}", delta=f"{delta:.2%}")
                elif len(df_kpi) == 1:
                    st.metric(label=f"EUR/{curr}", value=f"{df_kpi.iloc[0]['rate']:.4f}")

        st.markdown("### Évolution des Taux")
        # Graphique d'évolution multi-devises
        currencies_str = "','".join(selected_currencies)
        query_evo = f"""
            SELECT rate_date, target_currency, rate 
            FROM clean_forex 
            WHERE target_currency IN ('{currencies_str}')
            AND rate_date >= CURRENT_DATE - INTERVAL '{lookback_days} days'
            ORDER BY rate_date ASC
        """
        df_evo = load_data(query_evo)
        
        if not df_evo.empty:
            fig_evo = px.line(
                df_evo, x='rate_date', y='rate', color='target_currency',
                labels={'rate_date': 'Date', 'rate': 'Valeur (Base EUR)', 'target_currency': 'Devise'},
                template="plotly_dark",
                color_discrete_sequence=px.colors.qualitative.Safe
            )
            fig_evo.update_layout(hovermode="x unified")
            st.plotly_chart(fig_evo, use_container_width=True)
        
        # Volatilité
        st.markdown("### Profil de Volatilité (30 jours)")
        df_vol = load_data("SELECT target_currency, volatility_rel_pct, max_spread_pct FROM view_forex_volatility")
        if not df_vol.empty:
            # On ne garde que les devises sélectionnées pour le graph de volatilité
            df_vol_filtered = df_vol[df_vol['target_currency'].isin(selected_currencies)]
            fig_vol = px.bar(
                df_vol_filtered, x='target_currency', y='volatility_rel_pct',
                title="Volatilité Relative (%)",
                color='volatility_rel_pct',
                color_continuous_scale='RdYlGn_r',
                template="plotly_dark"
            )
            st.plotly_chart(fig_vol, use_container_width=True)

    # --- TAB 2: OBSERVABILITÉ PIPELINE ---
    with tab_pipeline:
        st.header("Santé du Flux de Données")
        
        # Santé Globale
        df_health = load_data("SELECT * FROM view_pipeline_health LIMIT 10")
        if not df_health.empty:
            latest_health = df_health.iloc[0]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Dernier Run", str(latest_health['run_day']))
            c2.metric("Taux de Succès", f"{latest_health['success_rate_pct']}%")
            c3.metric("Lignes Reçues", int(latest_health['total_received']))
            c4.metric("Lignes Rejetées", int(latest_health['total_rejected']), delta_color="inverse", delta=int(latest_health['total_rejected']))
            
            # Graphique d'historique de santé
            fig_health = px.area(
                df_health, x='run_day', y='success_rate_pct',
                title="Historique du Taux de Succès (%)",
                template="plotly_dark",
                range_y=[0, 105]
            )
            st.plotly_chart(fig_health, use_container_width=True)

        # Analyse des rejets
        st.subheader("Analyse de la Qualité (Rejets)")
        df_rejets = load_data("SELECT reason, COUNT(*) as count FROM rejects_forex GROUP BY reason")
        if not df_rejets.empty:
            fig_pie = px.pie(df_rejets, names='reason', values='count', title="Causes des échecs de validation", template="plotly_dark")
            st.plotly_chart(fig_pie, use_container_width=True)
            
            with st.expander("Voir le détail des rejets"):
                df_rejet_detail = load_data("SELECT * FROM rejects_forex ORDER BY rejected_at DESC LIMIT 50")
                st.dataframe(df_rejet_detail, use_container_width=True)
        else:
            st.success("Félicitations ! Aucune donnée n'a été rejetée récemment.")

    # --- TAB 3: AUDIT & RAW DATA ---
    with tab_audit:
        st.header("Audit Trail : Données Brutes")
        st.markdown("Consultez ici les derniers payloads reçus directement de l'API Frankfurter.")
        
        df_raw = load_data("SELECT id, payload, created_at FROM raw_forex ORDER BY created_at DESC LIMIT 10")
        if not df_raw.empty:
            for _, row in df_raw.iterrows():
                with st.expander(f"📦 ID: {row['id']} | Reçu le: {row['created_at']}"):
                    st.json(row['payload'])
        else:
            st.info("Aucune donnée brute trouvée dans la base.")

# --- FOOTER ---
st.divider()
st.caption(f"ForexFlow Dashboard v2.0 | Généré le {datetime.now().strftime('%d/%m/%Y %H:%M')}")
