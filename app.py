#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# App Streamlit (agregado por TIME + filtro Regional, com contato->cidade->filial->regional):
# - Fetch Intercom (conserva a busca original, adicionando "contacts" nos fields)
# - Para cada conversa: extrai contact.id -> GET /contacts/{id} -> location.city
# - Filial = "Mottu {city}" -> código da filial (dict) -> Regional (dict)
# - Filtro por Regional antes da agregação por Time
# - Exibe TABELA: Time | Qtd | TMA (ordenado por TMA desc)
# - Auto-refresh real a cada 10 min (meta refresh) + cache ttl=600
# - Tabela alta, width "stretch" e gradiente branco→vermelho no TMA (sem matplotlib)

import os, sys, json, requests, math, time, unicodedata
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

TIMEOUT = (5, 60)
PER_PAGE = 150
REFRESH_SECS = 600  # 10 min
EXCLUDE_ADMINS = {"Suporte Mottu", "Não atribuído"}

# -------------------------------------------------------------------
# MAPA HARDCODED: "Responsável" -> "Time"
TEAM_MAP_RAW: Dict[str, str] = {
    "App": "APP",
    "Gerbert Santos Santos": "APP",
    "Suporte Mottu": "BOT",
    "Pedro Henrique Stival": "CUSTOMER SUCCESS",
    "Thaís": "STORE",
    "Beatriz": "CUSTOMER SUCCESS",
    "Thiago Valcácio de Assis": "CS DUVIDAS",
    "João Pradella": "VENDAS",
    "Time de Acidentes": "FLEET ACIDENTES",
    "Felipe Ryu Nakamura": "FLEET DOCUMENTACOES",
    "Time Alerta": "FLEET DOCUMENTACOES",
    "Time de Ocorrências": "FLEET DOCUMENTACOES",
    "rogerio henrique correia filho": "FLEET PATIO",
    "Lucas Araújo Silva": "FLEET SUPORTE DE RUA",
    "Cristtiane Moreira": "FLEET SUPORTE DE RUA",
    "Gilberto Silva": "FLEET SUPORTE DE RUA",
    "Time Suporte de Rua": "FLEET SUPORTE DE RUA",
    "Engenharia": "MAINTENANCE",
    "MinhaMottu": "MINHA MOTTU",
    "João Ferrarini": "CREDITO",
    "Multas": "MULTAS",
    "Operações": "OPERATIONS",
    "Pagamentos": "PAYMENTS",
    "Bárbara Hülse": "PEOPLE",
    "Regulatório": "REGULATORIO",
    "Felipe Chao": "MINHA MOTTU",
    "Supply Chain": "SUPPLY",
    "Mottu Store": "STORE",
    "Ricardo Goes": "TOTEM ATTENDANCE",
    "Camila Camargo": "STORE",
    "Giulia": "OPERATIONS",
    "Patrick Freitas": "PARCERIAS E BENEFÍCIOS",
    "Apreensões - Pátio": "FLEET DOCUMENTACOES",
    "Thiago Castro": "FLEET PATIO",
    "Yasmin": "MINHA MOTTU",
    "Time de Acidentes": "SINISTRO"
}

# -------------------------------------------------------------------
# FILIAIS (nome -> código) e REGIONAIS (nome -> lista de códigos) hardcoded
filiais = {
    "Mottu Abaetetuba": 282, "Mottu Alagoinhas": 110, "Mottu Ananindeua": 122, "Mottu Anápolis": 58,
    "Mottu Aparecida de Goiânia": 123, "Mottu Aracaju": 29, "Mottu Aracati": 274, "Mottu Arapiraca": 52,
    "Mottu Araçatuba": 109, "Mottu Avaré": 454, "Mottu Barreiras": 259, "Mottu Bauru": 175,
    "Mottu Bayeux": 384, "Mottu Belo Horizonte": 3, "Mottu Belém": 18, "Mottu Blumenau": 356,
    "Mottu Boa Vista": 61, "Mottu Bragança": 238, "Mottu Brasília": 10, "Mottu Vila Leopoldina": 477,
    "Mottu Cabo Frio": 283, "Mottu Camaçari": 173, "Mottu Campina Grande": 38, "Mottu Campinas": 7,
    "Mottu Campo Grande": 31, "Mottu Campos dos Goytacazes": 285, "Mottu Caruaru": 39, "Mottu Cascavel": 397,
    "Mottu Castanhal": 365, "Mottu Caucaia": 458, "Mottu Caxias": 366, "Mottu Caxias do Sul": 69,
    "Mottu Colatina": 474, "Mottu Contagem": 53, "Mottu Crato": 295, "Mottu Criciúma": 51,
    "Mottu Cuiabá": 30, "Mottu Curitiba": 4, "Mottu Divinópolis": 174, "Mottu Dourados": 77,
    "Mottu Duque de Caxias": 469, "Mottu Eunápolis": 417, "Mottu Feira de Santana": 40, "Mottu Florianópolis": 32,
    "Mottu Fortaleza": 9, "Mottu Franca": 75, "Mottu Fátima": 114, "Mottu Goiânia": 15,
    "Mottu Governador Valadares": 76, "Mottu Guarulhos": 83, "Mottu Icoaraci": 404, "Mottu Imperatriz": 65,
    "Mottu Interlagos": 37, "Mottu Ipatinga": 55, "Mottu Ipiranga": 94, "Mottu Ipojuca": 267,
    "Mottu Itabuna": 116, "Mottu Itajaí": 111, "Mottu Itapetininga": 449, "Mottu Itapipoca": 357,
    "Mottu Jacarepaguá": 248, "Mottu Jandira": 41, "Mottu Jequié": 271, "Mottu Ji Paraná": 416,
    "Mottu Joinville": 56, "Mottu João Pessoa": 28, "Mottu Juazeiro": 45, "Mottu Juazeiro do Norte": 46,
    "Mottu Juiz de Fora": 95, "Mottu Jundiaí": 33, "Mottu Lagarto": 462, "Mottu Limão - Zona Norte": 36,
    "Mottu Linhares": 258, "Mottu Londrina": 49, "Mottu Macapá": 66, "Mottu Macaé": 266,
    "Mottu Maceió": 22, "Mottu Manaus": 5, "Mottu Marabá": 68, "Mottu Maracanaú": 180,
    "Mottu Maringá": 50, "Mottu Messejana": 402, "Mottu Mexico CDMX Cien Metros": 85,
    "Mottu Mexico CDMX Colegio Militar": 11, "Mottu Mexico CDMX Tlalpan": 71, "Mottu Mexico Cancún": 107,
    "Mottu Mexico Guadalajara": 47, "Mottu Mexico Guadalajara Centro": 113, "Mottu Mexico Los Reyes": 413,
    "Mottu Mexico Monterrey": 43, "Mottu Mexico Monterrey La Fe": 106, "Mottu Mexico Mérida": 249,
    "Mottu Mexico Puebla": 48, "Mottu Mexico Querétaro": 42, "Mottu Mexico Toluca": 459,
    "Mottu Mogi das Cruzes": 86, "Mottu Montes Claros": 57, "Mottu Mossoró": 67, "Mottu Natal": 27,
    "Mottu Niterói": 105, "Mottu Olinda": 84, "Mottu Palmas": 60, "Mottu Parauapebas": 79,
    "Mottu Parnamirim": 118, "Mottu Parnaíba": 115, "Mottu Patos": 300, "Mottu Pelotas": 203,
    "Mottu Petrolina": 309, "Mottu Pindamonhangaba": 311, "Mottu Piracicaba": 44, "Mottu Piçarreira": 183,
    "Mottu Ponta Grossa": 319, "Mottu Porto Alegre": 8, "Mottu Porto Seguro": 329, "Mottu Porto Velho": 59,
    "Mottu Pouso Alegre": 472, "Mottu Praia Grande": 82, "Mottu Presidente Prudente": 252,
    "Mottu Recife": 16, "Mottu Ribeirão Preto": 17, "Mottu Rio Branco": 62, "Mottu Rio Verde": 73,
    "Mottu Rondonópolis": 70, "Mottu Salvador": 6, "Mottu Santa Maria": 455, "Mottu Santarém": 81,
    "Mottu Santos": 24, "Mottu Serra": 19, "Mottu Sete Lagoas": 372, "Mottu Sobral": 74,
    "Mottu Sorocaba": 34, "Mottu São Bernardo": 23, "Mottu São Carlos": 64, "Mottu São José do Rio Preto": 63,
    "Mottu São José dos Campos": 20, "Mottu São Luís": 21, "Mottu São Miguel": 13, "Mottu Taboão": 35,
    "Mottu Teixeira de Freitas": 284, "Mottu Teresina": 26, "Mottu Toledo": 463, "Mottu Uberaba": 78,
    "Mottu Uberlândia": 25, "Mottu Valparaíso": 310, "Mottu Vila Isabel": 225, "Mottu Vila Velha": 72,
    "Mottu Vitória": 405, "Mottu Vitória da Conquista": 80, "Mottu Vitória de Santo Antão": 250,
    "Mottu Volta Redonda": 396, "Mottu Várzea Grande": 473, "Mottu Foz do Iguaçu": 511, "Mottu Passo Fundo": 522, "Mottu Sinop": 526,
    "Mottu Itumbiara": 537, "Mottu Lages": 527, "Mottu Patos de Minas": 509,
    "Mottu Cachoeiro de Itapemirim": 505, "Mottu Cariacica": 489, "Mottu Nossa Senhora do Socorro": 507,
    "Mottu Anápolis": 58, "Mottu MX Edomex Coacalco": 499,
    "Mottu México CDMX Iztapalapa": 87, "Mottu Campo Grande - RJ": 497,
    "Mottu São José do Ribamar": 513, "Mottu São Mateus": 514, "Mottu Ourinhos": 475, "Mottu Nova Iguaçu": 478, "Mottu Madureira": 476,
    "Mottu Poços de Caldas": 515, "Mottu Americana": 533,
    "Mottu Marília": 536, "Mottu Botucatu": 523, "Mottu Votuporanga": 542, "Mottu Varginha": 546, "Mottu Chapecó": 544,
    "Mottu Caxias": 366, "Mottu Ji Paraná": 416, "Mottu Itapetininga": 449,
    "Mottu Campos dos Goytacazes": 285, "Mottu Ponta Grossa": 319, "Mottu Cascavel": 397
}

# --- Normalização para mapear cidade -> código da filial (tolerante a acentos/caixa/espaços)
def _norm(s: str) -> str:
    s = "" if s is None else str(s).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))  # remove acentos
    s = " ".join(s.split())                                      # colapsa espaços
    return s.casefold()                                          # lower unicode

def _city_from_filial(name: str) -> str:
    name = str(name or "")
    return name[6:].strip() if name.lower().startswith("mottu ") else name.strip()

# Índice por CIDADE normalizada → código
CITY2CODE: Dict[str, int] = {}
for full_name, code in filiais.items():
    city = _city_from_filial(full_name)
    CITY2CODE[_norm(city)] = code
# (opcional) aliases se necessário
CITY_ALIASES = {
    # "campo grande rj": "Campo Grande - RJ",
    # "limao": "Limão - Zona Norte",
}
for k, v in CITY_ALIASES.items():
    if _norm(v) in CITY2CODE:
        CITY2CODE[_norm(k)] = CITY2CODE[_norm(v)]

regionais_base = {
    "Francisco": [61, 5, 59, 30, 4, 29, 28, 26, 27, 6, 21, 114, 9, 84, 16, 122, 18, 17],
    "Bruno": [31, 62, 66, 25, 68, 63, 81, 79, 38, 8, 3, 72, 19, 15, 118, 40, 46, 39],
    "Flávio": [82, 24, 35, 94, 83, 36, 23, 41, 477, 37, 13, 86, 7, 33, 34, 44],
    "Júlio": [22, 52, 57, 74, 67, 78, 116, 60, 65, 32, 111, 404, 56, 10, 45, 309, 53, 58, 123, 105, 402, 183, 173, 180, 20, 75],
    "Leonardo": [55, 474, 259, 285, 300, 77, 511, 416, 522, 526, 537, 527, 509, 455, 505, 357, 463, 397, 174, 203, 372, 473, 319, 489, 462, 507, 476, 478, 497, 396, 366, 513, 267, 514, 449, 454, 475, 472, 515, 533, 536, 523, 542, 546, 544],
    "Lucas": [42, 48, 107, 249, 113, 47, 43, 106, 71, 499, 459, 87, 413, 85, 11],
    "Rogério": [69, 70, 73, 76, 110, 115, 271, 274, 258, 329, 51, 95, 284, 252, 238, 80, 109, 49, 50, 310, 295, 405, 384, 225, 248, 266, 283, 250, 365, 282, 64, 175, 311, 469, 417, 356, 458]
}
regionais_ui = dict(regionais_base)
regionais_ui["Luciano"] = sorted(sum(regionais_base.values(), []))

# cria um mapa inverso código -> regional (apenas das regionais base)
code_to_regional: Dict[int, str] = {}
for reg_name, codes in regionais_base.items():
    for c in codes:
        code_to_regional[c] = reg_name

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
# Autenticação
# -------------------------
# Segredos (compat: [auth] OU chaves planas; c/ fallback em envvars)
def get_auth() -> dict:
    # 1) Tenta bloco [auth] em st.secrets
    if hasattr(st, "secrets") and "auth" in st.secrets:
        sect = st.secrets["auth"]
        bearer = (sect.get("INTERCOM_BEARER")).strip()
        base   = (sect.get("INTERCOM_BASE_URL")).strip()
        ver    = (sect.get("INTERCOM_VERSION")).strip()

    if not bearer:
        raise RuntimeError(
            "INTERCOM_BEARER ausente. Defina em 'Configurações → Advanced settings → Secrets' do Streamlit Cloud."
        )
    return {
        "INTERCOM_BEARER": bearer,
        "INTERCOM_BASE_URL": base,
        "INTERCOM_VERSION": ver,
    }

def _headers(auth: dict) -> dict:
    return {
        "Authorization": f"Bearer {auth['INTERCOM_BEARER']}",
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
    """Mantém busca original; adiciona 'contacts' nos fields para pegarmos o contact.id."""
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
            "conversations": ["id", "created_at", "state", "open", "admin_assignee_id", "contacts"]
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
        keep = {"id", "created_at", "state", "open", "admin_assignee_id", "contacts"}
        slim.extend({k: c.get(k) for k in keep} for c in batch)
        starting_after = (data.get("pagination") or {}).get("next")
        if not starting_after:
            break
    return slim

# -------------------------
# Contacts: cache simples em memória (por execução)
_CONTACT_CITY_CACHE: Dict[str, Optional[str]] = {}

def fetch_contact_city(base_url: str, hdrs: dict, contact_id: str) -> Optional[str]:
    """Busca city via /contacts/{id}. Cacheia resultados por execução."""
    if not contact_id:
        return None
    if contact_id in _CONTACT_CITY_CACHE:
        return _CONTACT_CITY_CACHE[contact_id]

    url = f"{base_url}/contacts/{contact_id}"
    try:
        r = requests.get(url, headers=hdrs, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json() or {}
        loc = data.get("location") or {}
        city = loc.get("city")
        _CONTACT_CITY_CACHE[contact_id] = city
        return city
    except Exception:
        _CONTACT_CITY_CACHE[contact_id] = None
        return None

# -------------------------
# Linhas detalhadas (um registro por ticket aberto) já com Regional
@st.cache_data(ttl=REFRESH_SECS, show_spinner=False)
def get_rows_with_regional() -> pd.DataFrame:
    auth = get_auth()
    base_url = (auth.get("INTERCOM_BASE_URL") or "https://api.intercom.io").rstrip("/")
    hdrs = _headers(auth)

    slim = fetch_conversations(base_url, hdrs)
    admin_map = fetch_admin_map(base_url, hdrs)

    now_ts = datetime.now(timezone.utc).timestamp()
    rows = []
    for obj in slim:
        if obj.get("state") != "open" or obj.get("open") is not True:
            continue

        # TMA
        try:
            ca = float(obj.get("created_at"))
        except Exception:
            continue
        tma_min = max(0.0, (now_ts - ca) / 60.0)

        # Responsável -> Time
        aid = obj.get("admin_assignee_id")
        admin_name = admin_map.get(str(aid)) if aid is not None else None
        resp = admin_name or "Não atribuído"
        if resp in EXCLUDE_ADMINS:
            continue
        time_group = map_to_team_or_self(resp)

        # Contact ID (pega o primeiro, se existir)
        contacts_struct = obj.get("contacts") or {}
        contact_list = contacts_struct.get("contacts") or []
        contact_id = None
        if contact_list and isinstance(contact_list, list):
            cid = contact_list[0] or {}
            contact_id = cid.get("id")

        # City -> Filial -> Código -> Regional
        city = fetch_contact_city(base_url, hdrs, contact_id) if contact_id else None
        city_norm = _norm(city) if city else ""
        filial_code = CITY2CODE.get(city_norm)
        filial_name = next((n for n, c in filiais.items() if c == filial_code), f"Mottu {city}" if city else "")
        regional = code_to_regional.get(filial_code, "NÃO MAPEADO")

        rows.append({
            "Time": time_group,
            "TMA_individual": tma_min,
            "Responsavel": resp,
            "ContactId": contact_id,
            "Cidade": city or "",
            "Filial": filial_name or "",
            "FilialCodigo": filial_code if filial_code is not None else "",
            "Regional": regional,
        })

    # garante colunas mesmo se vazio
    cols = ["Time", "TMA_individual", "Responsavel", "ContactId", "Cidade", "Filial", "FilialCodigo", "Regional"]
    return pd.DataFrame(rows, columns=cols)

def load_rows_with_progress() -> pd.DataFrame:
    bar = st.progress(0, text="Carregando dados…")

    auth = get_auth()
    base_url = (auth.get("INTERCOM_BASE_URL") or "https://api.intercom.io").rstrip("/")
    hdrs = _headers(auth)

    # 1) Conversas
    bar.progress(10, text="Carregando conversas…")
    slim = fetch_conversations(base_url, hdrs)

    # 2) Admins
    bar.progress(40, text="Mapeando responsáveis…")
    admin_map = fetch_admin_map(base_url, hdrs)

    # 3) Contatos/regionais (primeiro contato de cada conversa)
    bar.progress(70, text="Obtendo cidade/filial/regional…")
    contact_map = {}
    for obj in slim:
        contacts_struct = obj.get("contacts") or {}
        contact_list = contacts_struct.get("contacts") or []
        contact_id = None
        if contact_list and isinstance(contact_list, list):
            cid = contact_list[0] or {}
            contact_id = cid.get("id")
        city = fetch_contact_city(base_url, hdrs, contact_id) if contact_id else None
        city_norm = _norm(city) if city else ""
        filial_code = CITY2CODE.get(city_norm)
        filial_name = next((n for n, c in filiais.items() if c == filial_code), f"Mottu {city}" if city else "")
        regional = code_to_regional.get(filial_code, "NÃO MAPEADO")
        contact_map[contact_id] = {
            "Cidade": city or "",
            "Filial": filial_name or "",
            "FilialCodigo": filial_code if filial_code is not None else "",
            "Regional": regional,
        }

    # 4) Monta DataFrame final
    bar.progress(90, text="Finalizando…")
    now_ts = datetime.now(timezone.utc).timestamp()
    rows = []
    for obj in slim:
        if obj.get("state") != "open" or obj.get("open") is not True:
            continue
        try:
            ca = float(obj.get("created_at"))
        except Exception:
            continue
        tma_min = max(0.0, (now_ts - ca) / 60.0)
        aid = obj.get("admin_assignee_id")
        admin_name = admin_map.get(str(aid)) if aid is not None else None
        resp = admin_name or "Não atribuído"
        if resp in EXCLUDE_ADMINS:
            continue
        time_group = map_to_team_or_self(resp)

        contacts_struct = obj.get("contacts") or {}
        contact_list = contacts_struct.get("contacts") or []
        contact_id = None
        if contact_list and isinstance(contact_list, list):
            cid = contact_list[0] or {}
            contact_id = cid.get("id")

        cinfo = contact_map.get(contact_id, {})
        rows.append({
            "Time": time_group,
            "TMA_individual": tma_min,
            "Responsavel": resp,
            "ContactId": contact_id or "",
            "Cidade": cinfo.get("Cidade", ""),
            "Filial": cinfo.get("Filial", ""),
            "FilialCodigo": cinfo.get("FilialCodigo", ""),
            "Regional": cinfo.get("Regional", "NÃO MAPEADO"),
        })

    bar.progress(100, text="Concluído")
    cols = ["Time", "TMA_individual", "Responsavel", "ContactId", "Cidade", "Filial", "FilialCodigo", "Regional"]
    return pd.DataFrame(rows, columns=cols)

# -------------------------
# UI
st.set_page_config(page_title="Torre de Controle - CS Interno", layout="wide")

# Auto-refresh do navegador (rerun) a cada 10 minutos
st.markdown(f"<meta http-equiv='refresh' content='{REFRESH_SECS}'>", unsafe_allow_html=True)

# Compacta tabela para caber mais linhas
st.markdown(
    """
    <style>
      div[data-testid="stDataFrame"] table {
        font-size: 14px;
        line-height: 1.1;
      }
      div[data-testid="stDataFrame"] td, 
      div[data-testid="stDataFrame"] th {
        padding-top: 6px !important;
        padding-bottom: 6px !important;
        padding-left: 6px !important;
        padding-right: 6px !important;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Torre de Controle - CS Interno")

st.caption(f"Última atualização: {datetime.now().strftime('%H:%M:%S')}")

# Carrega linhas detalhadas
now = time.time()
if "rows_df" not in st.session_state or "expires_at" not in st.session_state or now >= st.session_state["expires_at"]:
    rows_df = load_rows_with_progress()          # barra única
    st.session_state["rows_df"] = rows_df
    st.session_state["expires_at"] = now + REFRESH_SECS
else:
    rows_df = st.session_state["rows_df"]

# -------------------------
# Filtro por REGIONAL (somente visual)
regionais_disponiveis = ["Todos"] + sorted(list(regionais_ui.keys())) + ["NÃO MAPEADO"]
regional_sel = st.selectbox("Filtrar por Regional", options=regionais_disponiveis, index=0)

if regional_sel != "Todos":
    rows_df = rows_df.loc[rows_df["Regional"] == regional_sel].copy()

# -------------------------
# Agregação por TIME sobre o subconjunto filtrado
if rows_df.empty:
    st.subheader("Dados em tempo real")
    st.info("Sem dados para exibir.")
else:
    agg = (
        rows_df.groupby("Time", dropna=False)
               .agg(Qtd=("TMA_individual", "size"), TMA=("TMA_individual", "mean"))
               .reset_index()
    )
    agg["TMA"] = pd.to_numeric(agg["TMA"], errors="coerce").round(2)
    agg = agg.sort_values(["TMA"], ascending=False, kind="stable").reset_index(drop=True)

    st.caption("Atualiza a cada 10 minutos. Tabela ordenada por TMA (min) do maior para o menor.")

    # Renderização com gradiente branco→vermelho no TMA
    df_display = agg.loc[:, ["Time", "Qtd", "TMA"]].copy()
    df_display["Qtd"] = pd.to_numeric(df_display["Qtd"], errors="coerce").fillna(0).astype(int)
    df_display["TMA"] = pd.to_numeric(df_display["TMA"], errors="coerce")

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
            t = 0.0 if t < 0 else (1.0 if t > 1 else t)
            g = int(round(255 * (1.0 - t)))  # branco -> vermelho
            styles.append(f"background-color: rgb(255,{g},{g}); color: black;")
        return styles

    styled = (
        df_display.style
        .format({"Qtd": "{:d}", "TMA": "{:.2f}"})
        .apply(red_white_gradient, subset=["TMA"])               # TMA com gradiente e texto preto
        .set_properties(subset=["Time", "Qtd"], **{"color": "white"})  # Time e Qtd com texto branco
        .set_properties(subset=["TMA"], **{"color": "black"})          # reforço texto preto em TMA
    )

    st.dataframe(
        styled,
        width="stretch",
        height=1000,
        hide_index=True,
        key="grid_times"
    )
