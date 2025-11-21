#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Torre de Controle - CS Interno (Streamlit)
# - Filial vem de custom_attributes["Lugar"] do contato
# - Mapeia Filial -> código -> Regional
# - Congela a tabela durante atualização (duplo buffer)
# - Atualiza automaticamente a cada 10 minutos (sem derrubar a tabela)
# - Horário exibido com ajuste de -3 horas (somente na UI)

import json, math, time, unicodedata
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import pandas as pd
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from map import TEAM_MAP_RAW, filiais, regionais_base

TIMEOUT = (5, 60)
PER_PAGE = 150
REFRESH_SECS = 600  # 10 minutos
EXCLUDE_ADMINS = {"Suporte Mottu", "Não atribuído"}

regionais_ui = dict(regionais_base)
regionais_ui["Luciano"] = sorted(sum(regionais_base.values(), []))

# mapa inverso código -> regional
code_to_regional: Dict[int, str] = {c: reg for reg, codes in regionais_base.items() for c in codes}

# -------------------------------------------------------------------
# Normalização e mapeamento de Time
def _normalize(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.casefold()

TEAM_MAP = {_normalize(k): v for k, v in TEAM_MAP_RAW.items() if str(k).strip()}

def map_to_team_or_self(responsavel: str) -> str:
    norm = _normalize(responsavel)
    if norm in TEAM_MAP and str(TEAM_MAP[norm]).strip():
        return str(TEAM_MAP[norm]).strip()
    return responsavel or "Não atribuído"

# -------------------------
# Helpers para Assunto / TMA / Descrição
def extract_assunto_from_tags(tags_block: Optional[dict]) -> str:
    """
    Escolhe a tag que tem pelo menos 3 hifens '-' e pega o texto entre o 2º e o 3º hífen.
    Ex.: 'CSINTERNO - ONBOARDING - ERRO AO ESCOLHER PLANO - HUMANO'
         -> 'ERRO AO ESCOLHER PLANO'
    """
    if not tags_block:
        return ""
    tags_list = tags_block.get("tags") or []
    main_name = ""
    for t in tags_list:
        name = (t.get("name") or "").strip()
        if name.count("-") >= 3:
            main_name = name
            break
    if not main_name:
        return ""
    parts = main_name.split("-")
    if len(parts) >= 4:
        return parts[2].strip()
    # fallback se vier fora do padrão
    return main_name

def extract_descricao(conv_custom_attrs: Optional[dict]) -> str:
    if not conv_custom_attrs:
        return ""
    return conv_custom_attrs.get("CS Interno - Descrição") or ""

# -------------------------
# Autenticação
def get_auth():
    if not getattr(st, "secrets", None) or "auth" not in st.secrets:
        raise RuntimeError("Segredos não encontrados. No Streamlit, crie .streamlit/secrets.toml com [auth].")
    auth = st.secrets["auth"]
    if not (auth.get("INTERCOM_BEARER")):
        raise RuntimeError("Chave ausente em [auth]: INTERCOM_BEARER")
    return auth

def _headers(auth: dict) -> dict:
    return {
        "Authorization": f"Bearer {(auth.get('INTERCOM_BEARER') or '').strip()}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Intercom-Version": auth.get("INTERCOM_VERSION", "2.14"),
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

# -------------------------
# Fetch Intercom
def fetch_admin_map(base_url: str, hdrs: dict) -> Dict[str, str]:
    url = f"{base_url}/admins"
    r = requests.get(url, headers=hdrs, timeout=TIMEOUT)
    r.raise_for_status()
    admins = (r.json() or {}).get("admins", [])
    return {str(a.get("id")): a.get("name") for a in admins if a.get("id") is not None}

def fetch_conversations(base_url: str, hdrs: dict) -> List[dict]:
    """
    Busca conversas abertas com a tag CSINTERNO.
    Inclui campos necessários para:
    - Time (admin_assignee_id -> admin_map -> TEAM_MAP)
    - Assunto (tags)
    - Descrição (custom_attributes da conversa)
    - TMA (statistics)
    - Contacts (para mapear Regional)
    """
    url = f"{base_url}/conversations/search"
    base_query = {
        "query": {
            "operator": "AND",
            "value": [
                {"field": "open", "operator": "=", "value": True},
                {"field": "state", "operator": "=", "value": "open"},
                {"field": "tag_ids", "operator": "IN", "value": ["11077847"]},
                {"field": "tag_ids", "operator": "NIN", "value": ["11457806"]}
            ],
        }
    }
    fields_block = {
        "fields": {
            "conversations": [
                "id",
                "created_at",
                "state",
                "open",
                "admin_assignee_id",
                "contacts",
                "tags",
                "statistics",
                "custom_attributes",
            ]
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
        slim.extend(batch)
        starting_after = (data.get("pagination") or {}).get("next")
        if not starting_after:
            break
    return slim

# -------------------------
# Contacts: cache simples por execução (Cidade/Lugar)
_CONTACT_INFO_CACHE: Dict[str, Dict[str, Optional[str]]] = {}

def fetch_contact_info(base_url: str, hdrs: dict, contact_id: str) -> Dict[str, Optional[str]]:
    """
    Retorna {"Cidade": <location.city ou "">, "Filial": <custom_attributes['Lugar'] ou "">}.
    """
    if not contact_id:
        return {"Cidade": "", "Filial": ""}
    if contact_id in _CONTACT_INFO_CACHE:
        return _CONTACT_INFO_CACHE[contact_id]

    url = f"{base_url}/contacts/{contact_id}"
    try:
        r = requests.get(url, headers=hdrs, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json() or {}
        loc = data.get("location") or {}
        cidade = loc.get("city") or ""
        ca = data.get("custom_attributes") or {}
        filial = ca.get("Lugar") or ca.get("lugar") or ca.get("LUGAR") or ""
        out = {"Cidade": cidade, "Filial": filial}
        _CONTACT_INFO_CACHE[contact_id] = out
        return out
    except Exception:
        out = {"Cidade": "", "Filial": ""}
        _CONTACT_INFO_CACHE[contact_id] = out
        return out

# -------------------------
# UI
# -------------------------
st.set_page_config(page_title="Torre de Controle - CS Interno", layout="wide")

# Rerun periódico
autorefresh_counter = st_autorefresh(interval=REFRESH_SECS * 1000, key="periodic_refresh")

# Debug temporário: ver se o autorefresh está rodando e qual o expires_at
st.caption(
    f"(debug) refresh_counter={autorefresh_counter} • "
    f"expires_at={st.session_state.get('expires_at')} • "
    f"now_ts={int(time.time())}"
)

# Estilo compacto
st.markdown(
    """
    <style>
      div[data-testid="stDataFrame"] table { font-size: 14px; line-height: 1.1; }
      div[data-testid="stDataFrame"] td, div[data-testid="stDataFrame"] th {
        padding-top: 6px !important; padding-bottom: 6px !important;
        padding-left: 6px !important; padding-right: 6px !important;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

# Estado do horário da última atualização (UTC epoch) — só muda ao final da coleta
if "last_refresh_ts" not in st.session_state:
    st.session_state["last_refresh_ts"] = None

def fmt_last_refresh_minus3() -> str:
    ts = st.session_state.get("last_refresh_ts")
    if ts is None:
        return "—"
    dt = datetime.fromtimestamp(ts, tz=timezone.utc) - timedelta(hours=3)
    return dt.strftime("%d/%m/%Y %H:%M:%S")

# ===== Cabeçalho (título + relógio) =====
header = st.container()
with header:
    st.title("Torre de Controle - CS Interno")
    caption_ph = st.empty()  # placeholder para o relógio

def update_caption():
    caption_ph.caption(f"Atualiza automaticamente a cada 10 minutos • Última atualização em: {fmt_last_refresh_minus3()}")

# Renderiza o relógio imediatamente (pode ser “—” na 1ª carga)
update_caption()

# ===== Barra de progresso no topo (visível durante a atualização) =====
progress_ph = st.empty()

# ===== Filtro (sempre antes da tabela) =====
top_controls = st.container()
with top_controls:
    regionais_disponiveis = ["Todos"] + sorted(list(regionais_ui.keys())) + ["NÃO MAPEADO"]
    mem = st.session_state.get("regional_sel_memory", "Todos")
    try:
        default_idx = regionais_disponiveis.index(mem) if mem in regionais_disponiveis else 0
    except Exception:
        default_idx = 0
    regional_sel = st.selectbox("Filtrar por Regional", options=regionais_disponiveis, index=default_idx)
    st.session_state["regional_sel_memory"] = regional_sel

# ===== Subtítulo + Cards + Tabela =====
subtitle_ph = st.empty()   # legenda da tabela
metrics_ph = st.empty()    # cartões
table_ph = st.empty()      # grade da tabela

def render_table(df: pd.DataFrame):
    """
    View:
    - Cards: TMA geral (min), Qtd tickets com TMA > 2h
    - Tabela: tickets com TMA > 20 min, ordenados por TMA (maior→menor),
      colunas: Time | Filial | Assunto | Descrição | TMA (min).
    """
    df = df.copy()

    # Filtro de regional
    if regional_sel != "Todos":
        df = df.loc[df["Regional"] == regional_sel].copy()

    # Sem dados
    if df.empty:
        subtitle_ph.empty()
        metrics_ph.empty()
        with table_ph.container():
            st.subheader("Dados em tempo real")
            st.info("Sem dados para exibir.")
        return

    # Garante TMA numérico
    df["TMA_min"] = pd.to_numeric(df["TMA_min"], errors="coerce")

    # Cards
    tma_geral = df["TMA_min"].mean(skipna=True)
    qtd_tma_maior_2h = (df["TMA_min"] > 120).sum()

    with metrics_ph.container():
        col1, col2 = st.columns(2)
        with col1:
            st.metric("TMA geral (min)", f"{tma_geral:.1f}" if pd.notna(tma_geral) else "—")
        with col2:
            st.metric("Qtd tickets com TMA > 2h", int(qtd_tma_maior_2h))

    # Tabela: TMA > 20 min, maior → menor
    df_tab = df[df["TMA_min"] > 20].copy()
    if df_tab.empty:
        subtitle_ph.caption("Nenhum ticket com TMA > 20 minutos para os filtros selecionados.")
        with table_ph.container():
            st.table(pd.DataFrame(columns=["Filial", "Time", "Assunto", "Descrição", "TMA (min)"]))
        return

    df_tab = df_tab.sort_values("TMA_min", ascending=False, kind="stable")

    subtitle_ph.caption("Tickets com TMA > 20 minutos, ordenados do maior para o menor TMA.")

    # Tabela final com Filial
    df_display = df_tab[["Filial", "Time", "Assunto", "Descrição", "TMA_min"]].rename(
        columns={"TMA_min": "TMA (min)"}
    )

    # Reset de índice (vamos tentar esconder depois)
    df_display = df_display.reset_index(drop=True)

    # ------------ CONFIG DE LARGURA POR COLUNA (AJUSTE AQUI) ------------
    COL_WIDTHS = {
        "Filial": "8rem",
        "Time": "14rem",
        "Assunto": "16rem",
        "Descrição": "32rem",
        "TMA (min)": "7rem",
    }
    # Ajuste esses valores se quiser mais/menos espaço em cada coluna
    # --------------------------------------------------------------------

    # Gradiente TMA
    def red_white_gradient(col: pd.Series):
        ser = pd.to_numeric(col, errors="coerce")
        vmin = float(ser.min())
        vmax = float(ser.max())
        if math.isnan(vmin) or math.isnan(vmax) or not math.isfinite(vmin) or not math.isfinite(vmax):
            return ["background-color: rgb(255,255,255); color: black;"] * len(col)
        rng = vmax - vmin
        if rng == 0:
            return ["background-color: rgb(255,255,255); color: black;"] * len(col)

        styles = []
        for v in ser:
            if v is None or math.isnan(v) or not math.isfinite(v):
                styles.append("")
                continue
            t = (v - vmin) / rng
            t = max(0.0, min(1.0, t))
            g = int(round(255 * (1.0 - t)))  # branco → vermelho
            styles.append(f"background-color: rgb(255,{g},{g}); color: black;")
        return styles

    styled = df_display.style

    # Formato TMA
    styled = styled.format({"TMA (min)": "{:.1f}"})

    # Gradiente em TMA
    styled = styled.apply(red_white_gradient, subset=["TMA (min)"])

    # Cores de texto
    styled = (
        styled
        .set_properties(subset=["Filial", "Time", "Assunto", "Descrição"], **{"color": "white"})
        .set_properties(subset=["TMA (min)"], **{"color": "black"})
    )

    # Tudo alinhado à esquerda
    styled = styled.set_properties(
        subset=df_display.columns,
        **{"text-align": "left"}
    )

    # Quebra de linha na descrição; linha cresce pra caber
    styled = styled.set_properties(
        subset=["Descrição"],
        **{
            "white-space": "normal",
            "word-wrap": "break-word",
            "overflow-wrap": "break-word",
        },
    )

    # Aplicar largura máxima configurada por coluna (sem min-width para não forçar scroll)
    for col, width in COL_WIDTHS.items():
        if col in df_display.columns:
            styled = styled.set_properties(
                subset=[col],
                **{
                    "max-width": width,
                },
            )

    # Tentar ocultar índice (nem sempre 100%, mas ajuda)
    try:
        styled = styled.hide(axis="index")
    except Exception:
        pass

    # CSS extra para tentar esconder cabeçalho/célula do índice em versões mais novas
    styled = styled.set_table_styles(
        [
            {"selector": "th.row_heading", "props": [("display", "none")]},
            {"selector": "td.row_heading", "props": [("display", "none")]},
            {"selector": "th.blank", "props": [("display", "none")]},
        ],
        overwrite=False,
    )

    with table_ph.container():
        st.table(styled)

# Exibe a última tabela conhecida; se não houver timestamp ainda, marca referência
rows_df_old = st.session_state.get("rows_df")
if rows_df_old is not None and st.session_state.get("last_refresh_ts") is None:
    st.session_state["last_refresh_ts"] = time.time()
    update_caption()
if rows_df_old is not None:
    render_table(rows_df_old)

# ===== Coleta com etapas e atualização do relógio =====
def collect_rows(progress_cb=None) -> pd.DataFrame:
    def step(p, txt):
        if callable(progress_cb):
            try:
                progress_cb(int(p), txt)
            except Exception:
                pass

    auth = get_auth()
    base_url = (auth.get("INTERCOM_BASE_URL") or "https://api.intercom.io").rstrip("/")
    hdrs = _headers(auth)

    # 1) Conversas
    step(10, "Carregando conversas…")
    slim = fetch_conversations(base_url, hdrs)

    # 2) Responsáveis
    step(35, "Mapeando responsáveis…")
    admin_map = fetch_admin_map(base_url, hdrs)

    # 3) Contatos → Lugar/Filial/Regional
    step(55, "Obtendo Lugar/Filial/Regional…")
    contact_map: Dict[str, Dict[str, Optional[str]]] = {}
    total = max(1, len(slim))
    for i, obj in enumerate(slim, start=1):
        contacts_struct = obj.get("contacts") or {}
        contact_list = contacts_struct.get("contacts") or []
        contact_id = (contact_list[0] or {}).get("id") if (contact_list and isinstance(contact_list, list)) else None

        if contact_id and contact_id not in _CONTACT_INFO_CACHE:
            cinfo = fetch_contact_info(base_url, hdrs, contact_id)
        else:
            cinfo = _CONTACT_INFO_CACHE.get(contact_id, {"Cidade": "", "Filial": ""}) if contact_id else {"Cidade": "", "Filial": ""}

        filial_name = cinfo.get("Filial") or ""
        filial_code = filiais.get(filial_name) if filial_name else None
        regional = code_to_regional.get(filial_code, "NÃO MAPEADO")

        contact_map[contact_id or ""] = {
            "Cidade": cinfo.get("Cidade", ""),
            "Filial": filial_name,
            "FilialCodigo": filial_code if filial_code is not None else "",
            "Regional": regional,
        }

        if i % 5 == 0 or i == total:
            frac = i / total
            pct = 55 + int(30 * frac)  # 55% → 85%
            step(pct, f"Obtendo Lugar/Filial/Regional… ({i}/{total})")

    # 4) Montagem por ticket
    step(90, "Finalizando…")
    rows = []
    now_ts = datetime.now(timezone.utc).timestamp()
    for obj in slim:
        if obj.get("state") != "open" or obj.get("open") is not True:
            continue

        # TMA = tempo em aberto = agora - created_at
        try:
            created_at = float(obj.get("created_at"))
        except Exception:
            continue  # se não tiver created_at válido, pula

        tma_min = max(0.0, (now_ts - created_at) / 60.0)

        # Responsável / Time
        aid = obj.get("admin_assignee_id")
        admin_name = admin_map.get(str(aid)) if aid is not None else None
        resp = admin_name or "Não atribuído"
        if resp in EXCLUDE_ADMINS:
            continue

        time_group = map_to_team_or_self(resp)

        # Filtro: remover Time = Supply (normalizado)
        if _normalize(time_group) == "supply":
            continue

        # Assunto via tags
        tags_block = obj.get("tags") or {}
        assunto = extract_assunto_from_tags(tags_block)

        # Descrição via custom_attributes da conversa
        conv_custom_attrs = obj.get("custom_attributes") or {}
        descricao = extract_descricao(conv_custom_attrs)

        # Contact / Regional
        contacts_struct = obj.get("contacts") or {}
        contact_list = contacts_struct.get("contacts") or []
        contact_id = (contact_list[0] or {}).get("id") if (contact_list and isinstance(contact_list, list)) else None
        cinfo = contact_map.get(contact_id or "", {})

        rows.append({
            "ConversationId": obj.get("id"),
            "Time": time_group,
            "Assunto": assunto,
            "Descrição": descricao,
            "TMA_min": tma_min,
            "Responsavel": resp,
            "ContactId": contact_id or "",
            "Cidade": cinfo.get("Cidade", ""),
            "Filial": cinfo.get("Filial", ""),
            "FilialCodigo": cinfo.get("FilialCodigo", ""),
            "Regional": cinfo.get("Regional", "NÃO MAPEADO"),
        })

    cols = [
        "ConversationId",
        "Time",
        "Assunto",
        "Descrição",
        "TMA_min",
        "Responsavel",
        "ContactId",
        "Cidade",
        "Filial",
        "FilialCodigo",
        "Regional",
    ]
    step(100, "Concluído")
    return pd.DataFrame(rows, columns=cols)

# Expiração (10 min)
now_ts = time.time()
expired = ("expires_at" not in st.session_state) or (now_ts >= st.session_state["expires_at"])

if expired or rows_df_old is None:
    # Barra de progresso VISÍVEL no topo com etapas reais
    bar = progress_ph.progress(0, text="Preparando atualização…")

    def progress_cb(pct, text):
        try:
            bar.progress(int(pct), text=text)
        except Exception:
            pass

    try:
        rows_df_new = collect_rows(progress_cb=progress_cb)
    except Exception as e:
        progress_ph.empty()
        st.error(f"Falha na atualização: {e}")
    else:
        # Grava novo estado e horário REAL da atualização
        st.session_state["rows_df"] = rows_df_new
        st.session_state["expires_at"] = time.time() + REFRESH_SECS
        st.session_state["last_refresh_ts"] = time.time()
        # Atualiza relógio imediatamente após terminar
        update_caption()
        progress_ph.empty()
        render_table(rows_df_new)
else:
    # Ainda válido: mantém tabela congelada, sem barra
    pass
