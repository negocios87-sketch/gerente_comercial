"""
Board Academy — Forecast Dashboard
Deploy: Render.com
"""

from flask import Flask, jsonify, request, session, redirect, render_template
import requests as req
import pandas as pd
import os
import unicodedata
import calendar
import math
from datetime import date, datetime, timedelta
from io import StringIO

app = Flask(__name__, template_folder='.')
app.secret_key = os.environ.get("SECRET_KEY", "boardacademy2026secret")

API_KEY           = os.environ.get("PIPE_API_KEY", "")

BASE_V1           = "https://boardacademy.pipedrive.com/api/v1"
BASE_V2           = "https://boardacademy.pipedrive.com/api/v2"
FILTER_DEALS      = int(os.environ.get("FILTER_DEALS",      "74674"))
FILTER_ACTIVITIES = int(os.environ.get("FILTER_ACTIVITIES", "1310451"))

CF_MULTIPLICADOR = "7e0e43c2734751f77be292a72527f638a850ad50"
CF_QUALIFICADOR  = "a6f13cc27c8d041f3af4091283ce0d4fe0913875"
CF_REUNIAO_VALID = "7299bf170c5deab9b4fd8c2275f55faf51984dea"

URL_COLAB    = os.environ.get("URL_COLAB",    "https://docs.google.com/spreadsheets/d/e/2PACX-1vSvwO3Ag2f2cbkVgR1pJZp6fANQcbualGKlAG50fmOljuEGKZ1gJBbSAjRdO3SomXUEVQOWnTvlfHRd/pub?gid=1782440078&single=true&output=csv")
URL_METAS    = os.environ.get("URL_METAS",    "https://docs.google.com/spreadsheets/d/e/2PACX-1vSvwO3Ag2f2cbkVgR1pJZp6fANQcbualGKlAG50fmOljuEGKZ1gJBbSAjRdO3SomXUEVQOWnTvlfHRd/pub?gid=0&single=true&output=csv")
URL_USERS    = os.environ.get("URL_USERS",    "https://docs.google.com/spreadsheets/d/e/2PACX-1vSvwO3Ag2f2cbkVgR1pJZp6fANQcbualGKlAG50fmOljuEGKZ1gJBbSAjRdO3SomXUEVQOWnTvlfHRd/pub?gid=160245570&single=true&output=csv")
URL_FERIADOS = os.environ.get("URL_FERIADOS", "https://docs.google.com/spreadsheets/d/e/2PACX-1vSvwO3Ag2f2cbkVgR1pJZp6fANQcbualGKlAG50fmOljuEGKZ1gJBbSAjRdO3SomXUEVQOWnTvlfHRd/pub?gid=1010928978&single=true&output=csv")

# ── HELPERS ───────────────────────────────────────────────────
def norm(s):
    if not s: return ""
    s = str(s).strip().lower()
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode()

SUPERUSERS_RAW = os.environ.get("SUPERUSERS", "farias souza")

def arred(v):
    try:
        f = float(v)
        return 0.0 if math.isnan(f) or math.isinf(f) else round(f, 2)
    except: return 0.0

def safe_div(a, b):
    try: return float(a) / float(b) if b else 0.0
    except: return 0.0

def cf(deal, key):
    val = deal.get(key)
    if val is None: return None
    if isinstance(val, dict): return val.get("value") or val.get("label")
    return val

def get_owner_name(deal):
    uid = deal.get("user_id")
    if isinstance(uid, dict): return uid.get("name", "")
    return ""

def get_owner_id(deal):
    uid = deal.get("user_id")
    if isinstance(uid, dict): return uid.get("id")
    return uid

def du_mes_total(ano, mes, feriados=set()):
    return sum(1 for d in range(1, calendar.monthrange(ano, mes)[1] + 1)
               if date(ano, mes, d).weekday() < 5 and date(ano, mes, d) not in feriados)

def du_passados(ano, mes, feriados=set()):
    hoje = date.today()
    return max(sum(1 for d in range(1, min(hoje.day, calendar.monthrange(ano, mes)[1]) + 1)
                   if date(ano, mes, d).weekday() < 5 and date(ano, mes, d) not in feriados), 1)

def du_restantes(ano, mes, feriados=set()):
    hoje = date.today()
    ultimo = calendar.monthrange(ano, mes)[1]
    return sum(1 for d in range(hoje.day + 1, ultimo + 1)
               if date(ano, mes, d).weekday() < 5 and date(ano, mes, d) not in feriados)

def limpar_nans(obj):
    if isinstance(obj, dict): return {k: limpar_nans(v) for k, v in obj.items()}
    if isinstance(obj, list): return [limpar_nans(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)): return None
    return obj

# ── SHEETS ────────────────────────────────────────────────────
def ler_sheet(url):
    resp = req.get(url, timeout=15)
    resp.encoding = "utf-8"
    resp.raise_for_status()
    return pd.read_csv(StringIO(resp.text))

def buscar_usuario(usuario, senha):
    df = ler_sheet(URL_USERS)
    df.columns = [c.strip().lower() for c in df.columns]
    for _, row in df.iterrows():
        if (norm(str(row.get("usuario", ""))) == norm(usuario) and
                str(row.get("senha", "")).strip() == str(senha).strip()):
            return str(row.get("usuario", ""))
    return None

def buscar_colaboradores():
    df = ler_sheet(URL_COLAB)
    df.columns = [c.strip() for c in df.columns]
    status_col = next((c for c in df.columns if "status" in norm(c)), None)
    if status_col:
        df = df[df[status_col].apply(lambda x: norm(str(x)) == "ativo")]
    return df

def buscar_feriados():
    try:
        df = ler_sheet(URL_FERIADOS)
        feriados = set()
        for _, row in df.iterrows():
            val = str(row.iloc[0]).strip()
            for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y"):
                try:
                    feriados.add(datetime.strptime(val, fmt).date())
                    break
                except: continue
        return feriados
    except: return set()

def buscar_metas_todas(ano, mes):
    df = ler_sheet(URL_METAS)
    df.columns = [c.strip() for c in df.columns]

    def to_num(v):
        try:
            if v is None: return 0.0
            if isinstance(v, float) and math.isnan(v): return 0.0
            return float(str(v).replace("R$","").replace(".","").replace(",",".").strip() or "0")
        except: return 0.0

    col_ano  = next((c for c in df.columns if norm(c) == "ano"), None)
    col_mes  = next((c for c in df.columns if norm(c) == "mes"), None)
    col_nome = next((c for c in df.columns if norm(c) == "nome"), None)
    col_reu  = next((c for c in df.columns if "reuni" in norm(c) and "meta" in norm(c)), None)
    col_fin  = next((c for c in df.columns if "financ" in norm(c)), None)
    col_du   = next((c for c in df.columns if "util" in norm(c)), None)

    rows = []
    for _, row in df.iterrows():
        try:
            a = int(float(str(row[col_ano]))) if col_ano else 0
            m = int(float(str(row[col_mes]))) if col_mes else 0
        except: continue
        if a != ano or m != mes: continue
        nome_raw = str(row[col_nome]).strip() if col_nome else ""
        meta_reu = to_num(row[col_reu]) if col_reu else 0.0
        meta_fin = (to_num(row[col_fin]) if col_fin else 0.0) / 10
        dias_ut  = 0
        if col_du:
            try: dias_ut = int(float(str(row[col_du] or 0)))
            except: dias_ut = 0
        rows.append({
            "nome": nome_raw, "nome_norm": norm(nome_raw),
            "meta_reu": meta_reu, "meta_fin": meta_fin, "dias_uteis": dias_ut,
        })
    return rows

# ── PIPEDRIVE ─────────────────────────────────────────────────
def buscar_users_pipe():
    resp = req.get(f"{BASE_V1}/users", params={"api_token": API_KEY}, timeout=15)
    resp.raise_for_status()
    return {u["id"]: u["name"] for u in (resp.json().get("data") or [])}

def buscar_qual_ids():
    resp = req.get(f"{BASE_V1}/dealFields", params={"api_token": API_KEY}, timeout=15)
    resp.raise_for_status()
    for field in (resp.json().get("data") or []):
        if field.get("key") == CF_QUALIFICADOR:
            return {norm(opt.get("label", "")): str(opt.get("id")) for opt in (field.get("options") or [])}
    return {}

def buscar_deals_mes(mes, ano):
    todos, start = [], 0
    mes_str = f"{ano}-{mes:02d}"
    while True:
        resp = req.get(f"{BASE_V1}/deals", params={
            "filter_id": FILTER_DEALS, "status": "won",
            "sort": "won_time DESC", "limit": 500,
            "start": start, "api_token": API_KEY,
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        lote = data.get("data") or []
        found_older = False
        for deal in lote:
            wt = str(deal.get("won_time", ""))[:7]
            if wt == mes_str: todos.append(deal)
            elif wt < mes_str: found_older = True
        mais = data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection", False)
        if not mais or not lote or found_older: break
        start += 500
    return todos

def buscar_activities_mes(mes, ano):
    todos, cursor = [], None
    mes_str = f"{ano}-{mes:02d}"
    while True:
        params = {"filter_id": FILTER_ACTIVITIES, "limit": 200}
        if cursor: params["cursor"] = cursor
        resp = req.get(f"{BASE_V2}/activities", params=params,
                       headers={"x-api-token": API_KEY}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        lote = data.get("data") or []
        for act in lote:
            if str(act.get("due_date", ""))[:7] == mes_str:
                todos.append(act)
        cursor = data.get("additional_data", {}).get("next_cursor")
        if not cursor or not lote: break
    return todos

# ── CÁLCULO ───────────────────────────────────────────────────

def buscar_deals_por_ids(deal_ids):
    """Busca deals individuais em paralelo para montar mapa_rv completo"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    mapa = {}
    if not deal_ids:
        return mapa

    def fetch_one(did):
        try:
            resp = req.get(f"{BASE_V1}/deals/{did}",
                params={"api_token": API_KEY}, timeout=8)
            if resp.status_code == 200:
                d = resp.json().get("data") or {}
                return did, cf(d, CF_REUNIAO_VALID)
        except:
            pass
        return did, None

    # Máximo 50 deals extras para evitar timeout
    ids_limitados = list(set(deal_ids))[:50]
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_one, did): did for did in ids_limitados}
        for future in as_completed(futures, timeout=20):
            try:
                did, rv = future.result()
                mapa[did] = rv
            except:
                pass
    return mapa

def calcular_abril(mes=None, ano=None, head_filter=None):
    hoje = date.today()
    mes  = mes or hoje.month
    ano  = ano or hoje.year

    feriados  = buscar_feriados()
    du_calc   = du_mes_total(ano, mes, feriados)
    du_pass   = du_passados(ano, mes, feriados)
    du_rest   = du_restantes(ano, mes, feriados)

    colab_df   = buscar_colaboradores()
    metas      = buscar_metas_todas(ano, mes)
    users_pipe = buscar_users_pipe()
    qual_ids   = buscar_qual_ids()
    deals      = buscar_deals_mes(mes, ano)
    activities = buscar_activities_mes(mes, ano)

    # Coluna flexível
    sub_col  = next((c for c in colab_df.columns if norm(c) == "subarea"), None)
    nome_col = next((c for c in colab_df.columns if norm(c) == "nome"), "Nome")
    head_col = next((c for c in colab_df.columns if "head" in norm(c)), None)

    # Mapas
    cargo_col = next((c for c in colab_df.columns if norm(c) == "cargo"), None)

    nome_to_subarea = {}
    nome_to_head    = {}
    nome_to_cargo   = {}
    for _, row in colab_df.iterrows():
        nn  = norm(str(row.get(nome_col, "")))
        sub = str(row.get(sub_col, "")).strip() if sub_col else ""
        hd  = str(row.get(head_col, "")).strip() if head_col else ""
        cg  = str(row.get(cargo_col, "")).strip() if cargo_col else ""
        nome_to_subarea[nn] = sub
        nome_to_head[nn]    = hd
        nome_to_cargo[nn]   = cg

    team_leaders = {nn for nn, cg in nome_to_cargo.items() if "team leader" in norm(cg) or "sales team leader" in norm(cg)}

    # Squads sem SDR — líderes desses squads vão para Closer, não SDR
    SQUADS_SEM_SDR = {"latam", "orion"}

    uid_to_nome      = {uid: name for uid, name in users_pipe.items()}
    uid_to_nome_norm = {uid: norm(name) for uid, name in users_pipe.items()}
    nome_norm_to_uid = {norm(name): uid for uid, name in users_pipe.items()}

    # Squads visíveis pelo head_filter
    if head_filter is None:
        squads_visiveis = None  # tudo
    elif head_filter == "__none__":
        squads_visiveis = set()  # nada
    elif head_filter.startswith("__squad__:"):
        sub_direto = norm(head_filter.replace("__squad__:", ""))
        squads_visiveis = {sub_direto}
    else:
        head_nn = norm(head_filter)
        squads_visiveis = set(
            norm(sub) for nn, sub in nome_to_subarea.items()
            if norm(nome_to_head.get(nn, "")) == head_nn and sub
        )

    def visivel(sub):
        return squads_visiveis is None or norm(sub) in squads_visiveis

    # ── Realizado por closer (owner) ──────────────────────────
    closer_real = {}
    for deal in deals:
        owner_nome = norm(get_owner_name(deal))
        if not owner_nome:
            oid = get_owner_id(deal)
            owner_nome = uid_to_nome_norm.get(oid, "")
        if not owner_nome: continue
        valor       = float(deal.get("value") or 0)
        valor_multi = float(cf(deal, CF_MULTIPLICADOR) or 0)
        if owner_nome not in closer_real:
            closer_real[owner_nome] = {"valor": 0, "valor_multi": 0, "qtd": 0}
        closer_real[owner_nome]["valor"]       += valor
        closer_real[owner_nome]["valor_multi"] += valor_multi
        closer_real[owner_nome]["qtd"]         += 1

    # ── Atividades por owner ──────────────────────────────────
    mapa_rv_ganhos = {d["id"]: cf(d, CF_REUNIAO_VALID) for d in deals}
    # Busca deals extras vinculados às activities que não estão nos ganhos do mês
    deal_ids_acts = [a.get("deal_id") for a in activities if a.get("deal_id")]
    ids_faltando  = [did for did in deal_ids_acts if did not in mapa_rv_ganhos]
    mapa_rv_extra = buscar_deals_por_ids(ids_faltando)
    mapa_rv = {**mapa_rv_ganhos, **mapa_rv_extra}
    acts_by_owner = {}
    for act in activities:
        oid = str(act.get("owner_id", ""))
        acts_by_owner.setdefault(oid, []).append(act)

    def act_valida(act):
        if not (act.get("done") is True or act.get("status") == "done"): return False
        rv = mapa_rv.get(act.get("deal_id"))
        return rv is None or str(rv).strip() == "" or norm(str(rv)) == "sim"

    # ── Metas por tipo ────────────────────────────────────────
    du_sheet = next((m["dias_uteis"] for m in metas if m["dias_uteis"] > 0), 0)
    du_total = du_sheet if du_sheet > 0 else du_calc

    closers_metas = [m for m in metas if m["meta_reu"] == 0 and m["meta_fin"] > 0]
    sdrs_metas    = [m for m in metas if m["meta_reu"] > 0  and m["meta_fin"] > 0]

    def build_closer_row(nome, meta, real, real_multi, qtd, is_head=False):
        mtd = safe_div(meta, du_total) * du_pass if du_total else 0
        return {
            "nome": nome, "meta": arred(meta), "is_head": is_head,
            "dias_uteis": du_total, "meta_du": arred(safe_div(meta, du_total)),
            "realizado": arred(real),
            "pct_atingido": arred(safe_div(real, meta) * 100),
            "mtd": arred(mtd), "deficit_mtd": arred(mtd - real),
            "pct_mtd": arred(safe_div(real, mtd) * 100),
            "deficit_meta": arred(meta - real),
            "meta_dia_100": arred(safe_div(meta - real, du_rest)) if du_rest else 0,
            "realizado_multi": arred(real_multi),
            "pct_atingido_multi": arred(safe_div(real_multi, meta) * 100),
            "deficit_meta_multi": arred(meta - real_multi),
            "meta_dia_multi": arred(safe_div(meta - real_multi, du_rest)) if du_rest else 0,
            "qtd_ganhos": qtd,
            "ticket_medio": arred(safe_div(real, qtd)) if qtd else 0,
        }

    # Coluna de líder
    lider_col = next((c for c in colab_df.columns if "lider" in norm(c) and "team" in norm(c)), None)

    # Mapa: nome_norm -> é líder de alguém
    lider_nomes = set()
    if lider_col:
        for _, row in colab_df.iterrows():
            lider_nome = norm(str(row.get(lider_col, "")))
            membro_nome = norm(str(row.get(nome_col, "")))
            if lider_nome and lider_nome != membro_nome:
                lider_nomes.add(lider_nome)

    # ── Organiza por Squad ─────────────────────────────────────
    squads = {}  # sub -> {closers: [], sdrs: [], nome: sub}

    def get_squad(sub):
        if sub not in squads:
            squads[sub] = {"nome": sub, "closers_ind": [], "sdrs_ind": [],
                           "closer_total": None, "sdr_total": None}
        return squads[sub]

    # Closers
    for m in closers_metas:
        nn  = m["nome_norm"]
        sub = nome_to_subarea.get(nn, "")
        if not sub or not visivel(sub): continue
        ri  = closer_real.get(nn, {"valor": 0, "valor_multi": 0, "qtd": 0})
        get_squad(sub)["closers_ind"].append(
            build_closer_row(m["nome"], m["meta_fin"], ri["valor"], ri["valor_multi"], ri["qtd"])
        )

    # Heads e Líderes de closer no próprio squad
    for uid, uname in users_pipe.items():
        nn      = norm(uname)
        own_sub = nome_to_subarea.get(nn, "")
        if not own_sub or not visivel(own_sub): continue
        if nn not in closer_real: continue
        # é head ou líder de closer (líder sem meta de reunião)
        is_head_of  = any(norm(nome_to_head.get(n2, "")) == nn for n2 in nome_to_subarea)
        is_lider_of = nn in lider_nomes and nn not in team_leaders
        # Team leader de squad sem SDR também vai pra closer
        is_tl_sem_sdr = nn in team_leaders and norm(own_sub) in SQUADS_SEM_SDR
        if not is_head_of and not is_lider_of and not is_tl_sem_sdr: continue
        existing = [norm(c["nome"]) for c in squads.get(own_sub, {}).get("closers_ind", [])]
        if nn in existing: continue
        ri = closer_real[nn]
        get_squad(own_sub)["closers_ind"].append(
            build_closer_row(uname, 0, ri["valor"], ri["valor_multi"], ri["qtd"], is_head=True)
        )

    # SDRs
    for m in sdrs_metas:
        nn  = m["nome_norm"]
        sub = nome_to_subarea.get(nn, "")
        if not sub or not visivel(sub): continue
        meta_reu = m["meta_reu"] / 10
        meta_fin = m["meta_fin"]
        uid      = nome_norm_to_uid.get(nn)
        uid_str  = str(uid) if uid else ""
        acts_sdr = acts_by_owner.get(uid_str, [])
        validadas     = [a for a in acts_sdr if act_valida(a)]
        qtd_val       = len(validadas)
        deveria_estar = arred(safe_div(meta_reu, du_total) * du_pass)
        pct_reu       = arred(safe_div(qtd_val, meta_reu) * 100)
        qual_id       = qual_ids.get(nn)
        deals_sdr     = [d for d in deals if str(cf(d, CF_QUALIFICADOR)) == str(qual_id)] if qual_id else []
        qtd_ganhos    = len(deals_sdr)
        valor_ganho   = sum(float(d.get("value") or 0) for d in deals_sdr)
        valor_multi   = sum(float(cf(d, CF_MULTIPLICADOR) or 0) for d in deals_sdr)
        pct_ganhos    = arred(safe_div(valor_multi, meta_fin) * 100)
        pct_final     = arred((pct_reu + pct_ganhos) / 2)
        get_squad(sub)["sdrs_ind"].append({
            "nome": m["nome"], "subarea": sub,
            "meta_reuniao": meta_reu,
            "meta_diaria": arred(safe_div(meta_reu, du_total)),
            "validadas": qtd_val,
            "deveria_estar": deveria_estar,
            "faltam": arred(deveria_estar - qtd_val),
            "pct_reu": pct_reu,
            "meta_ganho": arred(meta_fin),
            "qtd_ganhos": qtd_ganhos,
            "valor_ganho": arred(valor_ganho),
            "valor_ganho_multi": arred(valor_multi),
            "pct_ganhos": pct_ganhos,
            "ticket_medio": arred(safe_div(valor_ganho, qtd_ganhos)) if qtd_ganhos else 0,
            "pct_final": pct_final,
        })

    # ── Líderes de SDR sem meta mas com atividade/ganho ─────────
    sdr_nomes_ja = {norm(s["nome"]) for sq in squads.values() for s in sq["sdrs_ind"]}
    for uid, uname in users_pipe.items():
        nn = norm(uname)
        if nn not in lider_nomes and nn not in team_leaders: continue
        if nn in sdr_nomes_ja: continue
        if nn not in team_leaders: continue  # não é team leader, não vai pra SDR
        own_sub = nome_to_subarea.get(nn, "")
        if not own_sub or not visivel(own_sub): continue
        if norm(own_sub) in SQUADS_SEM_SDR: continue  # squad sem SDR, vai pra closer

        uid_str  = str(uid)
        acts_sdr = acts_by_owner.get(uid_str, [])
        validadas    = [a for a in acts_sdr if act_valida(a)]
        qtd_val      = len(validadas)

        qual_id      = qual_ids.get(nn)
        deals_sdr    = [d for d in deals if str(cf(d, CF_QUALIFICADOR)) == str(qual_id)] if qual_id else []
        qtd_ganhos   = len(deals_sdr)
        valor_ganho  = sum(float(d.get("value") or 0) for d in deals_sdr)
        valor_multi  = sum(float(cf(d, CF_MULTIPLICADOR) or 0) for d in deals_sdr)

        if qtd_val == 0 and qtd_ganhos == 0: continue  # nada a mostrar

        pct_final = 0.0  # sem meta, sem % calculado
        get_squad(own_sub)["sdrs_ind"].append({
            "nome": uname, "subarea": own_sub,
            "is_lider": True,
            "meta_reuniao": 0, "meta_diaria": 0,
            "validadas": qtd_val, "deveria_estar": 0,
            "faltam": 0, "pct_reu": 0,
            "meta_ganho": 0,
            "qtd_ganhos": qtd_ganhos,
            "valor_ganho": arred(valor_ganho),
            "valor_ganho_multi": arred(valor_multi),
            "pct_ganhos": 0,
            "ticket_medio": arred(safe_div(valor_ganho, qtd_ganhos)) if qtd_ganhos else 0,
            "pct_final": pct_final,
        })

    # ── Totais por Squad ──────────────────────────────────────
    def total_closers(ind):
        if not ind: return None
        t_meta = sum(c["meta"] for c in ind)
        t_real = sum(c["realizado"] for c in ind)
        t_multi= sum(c["realizado_multi"] for c in ind)
        t_qtd  = sum(c["qtd_ganhos"] for c in ind)
        return build_closer_row("TOTAL", t_meta, t_real, t_multi, t_qtd)

    def total_sdrs(ind):
        if not ind: return None
        t_reu  = sum(s["meta_reuniao"] for s in ind)
        t_val  = sum(s["validadas"] for s in ind)
        t_dev  = sum(s["deveria_estar"] for s in ind)
        t_mg   = sum(s["meta_ganho"] for s in ind)
        t_ganho= sum(s["valor_ganho"] for s in ind)
        t_multi= sum(s["valor_ganho_multi"] for s in ind)
        t_qtd  = sum(s["qtd_ganhos"] for s in ind)
        pct_r  = arred(safe_div(t_val, t_reu) * 100)
        pct_g  = arred(safe_div(t_multi, t_mg) * 100)
        return {
            "nome": "TOTAL", "subarea": "",
            "meta_reuniao": t_reu,
            "meta_diaria": arred(safe_div(t_reu, du_total)),
            "validadas": t_val, "deveria_estar": arred(t_dev),
            "faltam": arred(t_dev - t_val),
            "pct_reu": pct_r, "meta_ganho": arred(t_mg),
            "qtd_ganhos": t_qtd, "valor_ganho": arred(t_ganho),
            "valor_ganho_multi": arred(t_multi),
            "pct_ganhos": pct_g,
            "ticket_medio": arred(safe_div(t_ganho, t_qtd)) if t_qtd else 0,
            "pct_final": arred((pct_r + pct_g) / 2),
        }

    # Agrupa squads LIC-* em "Licenciados"
    squads_final = {}
    lic_closers = []
    lic_sdrs    = []
    squads_final = {}
    for sub, sq in squads.items():
        if sub.upper().startswith("LIC"):
            lic_closers.extend(sq["closers_ind"])
            lic_sdrs.extend(sq["sdrs_ind"])
        else:
            squads_final[sub] = sq

    if lic_closers or lic_sdrs:
        squads_final["Licenciados"] = {
            "nome": "Licenciados",
            "closers_ind": lic_closers,
            "sdrs_ind": lic_sdrs,
        }

    # ── Total consolidado ─────────────────────────────────────
    all_closers_ind = [c for sq in squads_final.values() for c in sq["closers_ind"]]
    all_sdrs_ind    = [s for sq in squads_final.values() for s in sq["sdrs_ind"]]
    total_geral_c   = total_closers(all_closers_ind)
    total_geral_s   = total_sdrs(all_sdrs_ind)

    # ── Resultado por Squad (usando squads_final com LIC agrupado) ──
    squads_result = []
    for sub, sq in squads_final.items():
        tc = total_closers(sq["closers_ind"])
        ts = total_sdrs(sq["sdrs_ind"])
        ating_closer = tc["pct_atingido_multi"] if tc else 0
        ating_sdr    = ts["pct_final"] if ts else None
        resultado    = arred((ating_closer + ating_sdr) / 2) if ating_sdr is not None else ating_closer
        squads_result.append({
            "nome": sq.get("nome", sub),
            "ating_closer": arred(ating_closer),
            "ating_sdr": arred(ating_sdr) if ating_sdr is not None else None,
            "resultado": arred(resultado),
            "tem_sdr": ts is not None,
            # Closer values
            "closer_bruto":  arred(tc["realizado"]) if tc else 0,
            "closer_multi":  arred(tc["realizado_multi"]) if tc else 0,
            "closer_vol":    tc["qtd_ganhos"] if tc else 0,
            # SDR values
            "sdr_bruto":     arred(ts["valor_ganho"]) if ts else 0,
            "sdr_multi":     arred(ts["valor_ganho_multi"]) if ts else 0,
            "sdr_reunioes":  ts["validadas"] if ts else 0,
        })

    # Serializa squads
    squads_out = []
    for sub, sq in squads_final.items():
        tc = total_closers(sq["closers_ind"])
        ts = total_sdrs(sq["sdrs_ind"])
        squads_out.append({
            "nome": sq.get("nome", sub),
            "closers": sq["closers_ind"],
            "closer_total": tc,
            "sdrs": sq["sdrs_ind"],
            "sdr_total": ts,
        })

    # ── Card consolidado Denise (Elite + Sniper + MGM/Olympus) ──
    DENISE_SQUADS = {"elite", "sniper", "mgm", "olympus"}
    denise_squads = [r for r in squads_result if norm(r["nome"]) in DENISE_SQUADS]
    if denise_squads:
        d_closer = arred(safe_div(
            sum(sq["ating_closer"] for sq in denise_squads),
            len(denise_squads)
        ))
        d_sdr_vals = [sq["ating_sdr"] for sq in denise_squads if sq["ating_sdr"] is not None]
        d_sdr = arred(sum(d_sdr_vals) / len(d_sdr_vals)) if d_sdr_vals else None
        d_resultado = arred((d_closer + d_sdr) / 2) if d_sdr is not None else d_closer
        squads_result.append({
            "nome": "Denise Mussolin",
            "ating_closer": d_closer,
            "ating_sdr": d_sdr,
            "resultado": d_resultado,
            "tem_sdr": d_sdr is not None,
            "is_consolidated": True,
            "closer_bruto":  arred(sum(sq.get("closer_bruto", 0) for sq in denise_squads)),
            "closer_multi":  arred(sum(sq.get("closer_multi", 0) for sq in denise_squads)),
            "closer_vol":    sum(sq.get("closer_vol", 0) for sq in denise_squads),
            "sdr_bruto":     arred(sum(sq.get("sdr_bruto", 0) for sq in denise_squads)),
            "sdr_multi":     arred(sum(sq.get("sdr_multi", 0) for sq in denise_squads)),
            "sdr_reunioes":  sum(sq.get("sdr_reunioes", 0) for sq in denise_squads),
        })

    # Debug Larissa
    larissa_nn = norm("Larissa Vitor")
    _debug_larissa = {
        "in_closer_real": larissa_nn in closer_real,
        "in_team_leaders": larissa_nn in team_leaders,
        "in_lider_nomes": larissa_nn in lider_nomes,
        "own_sub": nome_to_subarea.get(larissa_nn, "NAO ENCONTRADO"),
        "closer_real_value": closer_real.get(larissa_nn, "NAO ENCONTRADO"),
        "nome_to_cargo": nome_to_cargo.get(larissa_nn, "NAO ENCONTRADO"),
        "latam_closers": [c["nome"] for sq in squads.values() if norm(sq.get("nome","")) == "latam" or True for c in sq.get("closers_ind",[])],
    }

    return {
        "periodo": {
            "mes": mes, "ano": ano,
            "du_total": du_total, "du_passados": du_pass, "du_restantes": du_rest,
            "atualizado_em": (datetime.now() - timedelta(hours=3)).strftime("%d/%m/%Y %H:%M"),
        },
        "squads": squads_out,
        "resultados": squads_result,
        "total_geral": {
            "closer": total_geral_c,
            "sdr": total_geral_s,
        },
    }

# ── ROTAS ─────────────────────────────────────────────────────
@app.route("/")
def index():
    return redirect("/login" if "nome" not in session else "/abril")

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        usuario = request.form.get("usuario", "").strip()
        senha   = request.form.get("senha",   "").strip()
        nome    = buscar_usuario(usuario, senha)
        if nome:
            session["nome"] = nome
            return redirect("/abril")
        return render_template("login.html", erro="Usuário ou senha inválidos"), 401
    return render_template("login.html", erro=None)

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")

@app.route("/abril")
def abril():
    if "nome" not in session:
        return redirect("/login")
    return render_template("abril.html", nome=session["nome"])

@app.route("/api/abril")
def api_abril():
    if "nome" not in session:
        return jsonify({"erro": "Não autenticado"}), 401
    try:
        mes  = request.args.get("mes", type=int)
        ano  = request.args.get("ano", type=int)
        nome_sess = session.get("nome", "")

        # Detecta gerente: quem não tem head (ou é head de si mesmo)
        colab_df  = buscar_colaboradores()
        head_col  = next((c for c in colab_df.columns if "head" in norm(c)), None)
        nome_col  = next((c for c in colab_df.columns if norm(c) == "nome"), "Nome")
        # Superusuários sempre veem tudo
        superusers = {norm(u.strip()) for u in SUPERUSERS_RAW.split(",")}
        nn_sess = norm(nome_sess)

        if nn_sess in superusers:
            # Superuser vê tudo
            head_filter = None
        else:
            # Verifica se é Head
            is_head = False
            if head_col:
                for _, row in colab_df.iterrows():
                    if norm(str(row.get(head_col, ""))) == nn_sess:
                        is_head = True
                        break

            if is_head:
                # Head vê os squads onde é Head
                head_filter = nome_sess
            else:
                # Verifica se é Líder
                lider_col_l = next((c for c in colab_df.columns if "lider" in norm(c) and "team" in norm(c)), None)
                is_lider = False
                lider_sub = None
                if lider_col_l:
                    for _, row in colab_df.iterrows():
                        if norm(str(row.get(lider_col_l, ""))) == nn_sess:
                            sub = str(row.get(sub_col if (sub_col := next((c for c in colab_df.columns if norm(c) == "subarea"), None)) else "Subárea", "")).strip()
                            if sub:
                                lider_sub = sub
                                is_lider = True
                                break

                if is_lider and lider_sub:
                    # Líder vê só o próprio squad — usa squad_filter especial
                    head_filter = f"__squad__:{lider_sub}"
                else:
                    # Pessoa sem cargo especial — não vê nada
                    head_filter = "__none__"
        return jsonify(limpar_nans(calcular_abril(mes=mes, ano=ano, head_filter=head_filter)))
    except Exception as e:
        import traceback
        return jsonify({"erro": str(e), "trace": traceback.format_exc()}), 500

@app.route("/api/debug/metas")
def debug_metas():
    if "nome" not in session:
        return jsonify({"erro": "Não autenticado"}), 401
    hoje = date.today()
    df = ler_sheet(URL_METAS)
    return jsonify({
        "colunas": list(df.columns),
        "primeiras_5_linhas": df.head(5).fillna("").to_dict(orient="records"),
        "mes_ano": f"{hoje.month}/{hoje.year}",
    })

@app.route("/api/debug/colab")
def debug_colab():
    if "nome" not in session:
        return jsonify({"erro": "Não autenticado"}), 401
    df = ler_sheet(URL_COLAB)
    return jsonify({
        "colunas": list(df.columns),
        "primeiras_5_linhas": df.head(5).fillna("").to_dict(orient="records"),
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
