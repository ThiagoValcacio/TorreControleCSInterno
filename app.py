#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# App Streamlit:
# - NÃO altera a obtenção dos dados (mesma query/fields e chamada a /admins)
# - Constrói um DataFrame agregado por admin_name: ['Admin_name','TMA','qtd']
# - Gráfico único: barra (TMA, verde) + linha (Qtd, vermelha)
# - Fundo do gráfico transparente (herda a cor padrão do app)
# - Animação de "pulso" nas barras por alguns segundos
# - Auto-refresh a cada 5 minutos (st.rerun) e cache (ttl=300)

import os, sys, json, requests, math, time
from datetime import datetime, timezone
from typing import Dict, List

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

TIMEOUT = (5, 30)
PER_PAGE = 150
REFRESH_SECS = 300  # 5 min
EXCLUDE_ADMINS = {"Suporte Mottu", "Não atribuído"} 

# -------------------------
# Infra básica (MESMO fetch)
# -------------------------
def load_secrets(path: str) -> dict:
    try:
        import tomllib  # Py>=3.11
        with open(path, "rb") as f:
            return tomllib.load(f)
    except ModuleNotFoundError:
        import toml
        with open(path, "r", encoding="utf-8") as f:
            return toml.load(f)
        
def get_auth():
    """
    Lê segredos do Streamlit Cloud (st.secrets) ou falha com mensagem clara.
    Espera o bloco:
      [auth]
      INTERCOM_BEARER="..."
      INTERCOM_VERSION="2.14"
      INTERCOM_BASE_URL="https://api.intercom.io"
    """
    if not getattr(st, "secrets", None) or "auth" not in st.secrets:
        raise RuntimeError(
            "Segredos não encontrados. No Streamlit Cloud, abra Settings → Secrets e adicione o bloco [auth]."
        )
    auth = st.secrets["auth"]
    for k in ("INTERCOM_BEARER",):
        if not auth.get(k):
            raise RuntimeError(f"Chave ausente em [auth]: {k}")
    return auth

def make_headers(auth: dict) -> dict:
    bearer = (auth.get("INTERCOM_BEARER") or "").strip()
    if not bearer:
        raise RuntimeError("INTERCOM_BEARER ausente em [auth] do secrets.toml")
    return {
        "Authorization": f"Bearer {bearer}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Intercom-Version": auth.get("INTERCOM_VERSION", "2.14"),
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

def fetch_admin_map(base_url: str, hdrs: dict) -> Dict[str, str]:
    url = f"{base_url}/admins"
    r = requests.get(url, headers=hdrs, timeout=TIMEOUT)
    r.raise_for_status()
    admins = (r.json() or {}).get("admins", [])
    return {str(a.get("id")): a.get("name") for a in admins if a.get("id") is not None}

def fetch_conversations(base_url: str, hdrs: dict) -> List[dict]:
    """Conserva a MESMA obtenção: filtro + fields + paginação por cursor."""
    url = f"{base_url}/conversations/search"

    base_query = {
        "query": {
            "operator": "AND",
            "value": [
                {"field": "open", "operator": "=", "value": True},
                {"field": "state", "operator": "=", "value": "open"},
                {"field": "tag_ids", "operator": "IN", "value": ["11077847"]},
            ],
        }
    }
    fields_block = {
        "fields": {
            "conversations": ["id", "created_at", "state", "open", "admin_assignee_id"]
        }
    }

    slim = []
    starting_after = None
    while True:
        body = {**base_query, **fields_block, "pagination": {"per_page": PER_PAGE}}
        if starting_after:
            body["pagination"]["starting_after"] = starting_after

        r = requests.post(url, headers=hdrs, json=body, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        batch = data.get("conversations") or data.get("data") or []
        if not batch:
            break

        keep = {"id", "created_at", "state", "open", "admin_assignee_id"}
        slim.extend({k: c.get(k) for k in keep} for c in batch)

        starting_after = (data.get("pagination") or {}).get("next")
        if not starting_after:
            break

    return slim

# -------------------------
# Cálculo e DataFrame
# -------------------------
@st.cache_data(ttl=REFRESH_SECS)
def get_df():
    auth = get_auth()
    base_url = (auth.get("INTERCOM_BASE_URL") or "https://api.intercom.io").rstrip("/")
    hdrs = {
        "Authorization": f"Bearer {(auth.get('INTERCOM_BEARER') or '').strip()}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Intercom-Version": auth.get("INTERCOM_VERSION", "2.14"),
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

    slim = fetch_conversations(base_url, hdrs)
    admin_map = fetch_admin_map(base_url, hdrs)

    now_ts = datetime.now(timezone.utc).timestamp()
    rows = []
    for obj in slim:
        if obj.get("state") != "open" or obj.get("open") is not True:
            continue
        ca = obj.get("created_at")
        try:
            ca = float(ca)
        except Exception:
            continue

        tma_min = max(0.0, (now_ts - ca) / 60.0)
        aid = obj.get("admin_assignee_id")
        admin_name = admin_map.get(str(aid)) if aid is not None else None
        name = admin_name or "Não atribuído"

        # --- filtro para remover "Suporte Mottu" ---
        if name in EXCLUDE_ADMINS:
            continue

        rows.append({"Admin_name": name, "TMA_individual": tma_min})

    if not rows:
        return pd.DataFrame(columns=["Admin_name", "TMA", "qtd"])

    df = pd.DataFrame(rows)
    agg = (
        df.groupby("Admin_name", dropna=False)
          .agg(qtd=("TMA_individual", "size"), TMA=("TMA_individual", "mean"))
          .reset_index()
    )
    agg["TMA"] = agg["TMA"].round(2)
    # ordenar pelo maior TMA
    agg = agg.sort_values(["TMA"], ascending=False, kind="stable").reset_index(drop=True)
    return agg  # colunas: Admin_name, TMA, qtd

# -------------------------
# Gráfico com pulso
# -------------------------
def _make_figure(df):
    # Dados
    x_labels = df["Admin_name"].tolist()
    x = range(len(x_labels))
    tma = df["TMA"].tolist()
    qtd = df["qtd"].tolist()

    # Figura e eixos transparentes (herdam fundo do app)
    fig, ax1 = plt.subplots(figsize=(12, 6))
    fig.patch.set_alpha(0.0)
    ax1.set_facecolor((0, 0, 0, 0))

    # Contornos e textos brancos
    for spine in ax1.spines.values():
        spine.set_color("white")
    ax1.tick_params(colors="white")
    ax1.set_ylabel("TMA (min)", color="white")

    # Barras verdes (TMA) com contorno branco
    ax1.bar(x, tma, color="#00C853", edgecolor="white", linewidth=1.0)

    # Linha (Qtd) – eixo secundário
    ax2 = ax1.twinx()
    ax2.set_facecolor((0, 0, 0, 0))
    for spine in ax2.spines.values():
        spine.set_color("white")
    ax2.tick_params(colors="white")
    ax2.set_ylabel("Qtd", color="white")
    ax2.plot(x, qtd, color="#FF1744", marker="o", linewidth=2.0)

    # Eixo X: rótulos na vertical (brancos)
    ax1.set_xticks(list(x))
    ax1.set_xticklabels(x_labels, rotation=90, ha="right", color="white")

    # Grade discreta branca
    ax1.grid(color="white", alpha=0.15, linestyle="--", linewidth=0.5)

    plt.tight_layout()
    return fig

def plot_combo(df):
    fig = _make_figure(df)
    st.pyplot(fig)
    plt.close(fig)

# -------------------------
# UI Streamlit
# -------------------------
st.set_page_config(page_title="Torre de Controle - CS Interno", layout="wide")

now = time.time()
if "last_refresh_ts" not in st.session_state:
    st.session_state["last_refresh_ts"] = now

# placeholder do badge dinâmico
badge_ph = st.empty()

remaining = max(0, int(REFRESH_SECS - (now - st.session_state["last_refresh_ts"])))

def render_badge(remaining: int):
    st.components.v1.html(
        f"""
        <style>
          .pulse-badge {{
            display: inline-flex; align-items: center; gap: 8px;
            padding: 6px 12px; border: 1px solid #FFFFFF; border-radius: 9999px;
            color: #FFFFFF; background: rgba(255,255,255,0.06);
            box-shadow: 0 0 0 0 rgba(0,200,83,0.6);
            animation: pulseShadow 2s infinite;
            font-size: 0.9rem; font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
          }}
          .pulse-dot {{
            width: 10px; height: 10px; border-radius: 50%;
            background: #00C853; box-shadow: 0 0 8px #00C853;
            animation: pulseDot 1.5s infinite;
          }}
          @keyframes pulseShadow {{
            0% {{ box-shadow: 0 0 0 0 rgba(0,200,83,0.6); }}
            70% {{ box-shadow: 0 0 0 14px rgba(0,200,83,0); }}
            100% {{ box-shadow: 0 0 0 0 rgba(0,200,83,0); }}
          }}
          @keyframes pulseDot {{
            0%,100% {{ transform: scale(1); }}
            50% {{ transform: scale(1.25); }}
          }}
        </style>
        <div class="pulse-badge">
          <span class="pulse-dot"></span>
          <span><strong id="status">Atualizando</strong> · próximo em <span id="countdown">{remaining}</span>s</span>
        </div>
        <script>
          (function() {{
            var remain = {remaining};
            var el = document.getElementById('countdown');
            var st = document.getElementById('status');
            if (st) st.textContent = "Atualizando";
            function tick() {{
              if (remain > 0) {{
                remain -= 1;
                if (el) el.textContent = remain;
                if (st) st.textContent = "Atualizando";
              }} else {{
                // Chegou a zero: apenas mostra "Atualizando…"
                if (st) st.textContent = "Atualizando…";
                clearInterval(timer);
              }}
            }}
            var timer = setInterval(tick, 1000);
          }})();
        </script>
        """,
        height=48,
    )

render_badge(remaining)

st.title("Torre de Controle - CS Interno")

# dispara um rerun quando passar de 5 min
last = st.session_state.get("last_refresh_ts", 0)
if now - last >= REFRESH_SECS:
    st.session_state["last_refresh_ts"] = now
    st.rerun()

df = get_df()

st.subheader("TMA e Qtd Tickets Ativos por Responsável")
st.caption("Atualiza a cada 5 minutos. Barras = TMA (min) | Linha = Qtd de Tickets.")
if df.empty:
    st.info("Sem dados para exibir.")
else:
    # 6s de pulso leve; ajuste seconds/fps/amp/freq se quiser
    plot_combo(df)

