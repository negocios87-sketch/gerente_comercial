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
from datetime import date, datetime
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

URL_COLAB = os.environ.get("URL_COLAB", "https://docs.google.com/spreadsheets/d/e/2PACX-1vSvwO3Ag2f2cbkVgR1pJZp6fANQcbualGKlAG50fmOljuEGKZ1gJBbSAjRdO3SomXUEVQOWnTvlfHRd/pub?gid=1782440078&single=true&output=csv")
URL_METAS = os.environ.get("URL_METAS", "https://docs.google.com/spreadsheets/d/e/2PACX-1vSvwO3Ag2f2cbkVgR1pJZp6fANQcbualGKlAG50fmOljuEGKZ1gJBbSAjRdO3SomXUEVQOWnTvlfHRd/pub?gid=0&single=true&output=csv")
URL_USERS = os.environ.get("URL_USERS", "https://docs.google.com/spreadsheets/d/e/2PACX-1vSvwO3Ag2f2cbkVgR1pJZp6fANQcbualGKlAG50fmOljuEGKZ1gJBbSAjRdO3SomXUEVQOWnTvlfHRd/pub?gid=160245570&single=true&output=csv")

def norm(s):
    if not s: return ""
    s = str(s).strip().lower()
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode()

def arred(v):
    try: return round(float(v), 2)
    except: return 0.0

def safe_div(a, b):
    try: return float(a) / float(b) if b else 0.0
    except: return 0.0

def cf(deal, key):
    val = deal.get(key)
    if val is None: return None
    if isinstance(val, dict): return val.get("value") or val.get("label")
    return val

def get_owner_id(deal):
    uid = deal.get("user_id")
    if isinstance(uid, dict): return uid.get("id")
    return uid

def get_owner_name(deal):
    uid = deal.get("user_id")
    if isinstance(uid, dict): return uid.get("name", "")
    return ""

def du_mes_total(ano, mes):
    return sum(1 for d in range(1, calendar.monthrange(ano, mes)[1] + 1)
               if date(ano, mes, d).weekday() < 5)

def du_passados(ano, mes):
    hoje = date.today()
    return max(sum(1 for d in range(1, min(hoje.day, calendar.monthrange(ano, mes)[1]) + 1)
                   if date(ano, mes, d).weekday() < 5), 1)

def du_restantes(ano, mes):
    hoje = date.today()
    ultimo = calendar.monthrange(ano, mes)[1]
    return sum(1 for d in range(hoje.day + 1, ultimo + 1)
               if date(ano, mes, d).weekday() < 5)

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
    status_col = next((c for c in df.columns if "status" in c.lower()), None)
    if status_col:
        df = df[df[status_col].apply(lambda x: norm(str(x)) == "ativo")]
    return df

def buscar_metas_todas(ano, mes):
    df = ler_sheet(URL_METAS)
    df.columns = [c.strip() for c in df.columns]

    def to_num(v):
        import math
        try:
            if v is None: return 0.0
            if isinstance(v, float) and math.isnan(v): return 0.0
            return float(str(v).replace("R$","").replace(".","").replace(",",".").strip() or "0")
        except: return 0.0

    col_ano  = next((c for c in df.columns if norm(c) == "ano"), None)
    col_mes  = next((c for c in df.columns if norm(c) in ["mes","mes"]), None)
    col_nome = next((c for c in df.columns if norm(c) == "nome"), None)
    col_reu  = next((c for c in df.columns if "reuni" in norm(c) and "meta" in norm(c)), None)
    col_fin  = next((c for c in df.columns if "financ" in norm(c)), None)
    col_du   = next((c for c in df.columns if "util" in norm(c) or "uteis" in norm(c)), None)

    rows = []
    for _, row in df.iterrows():
        try:
            a = int(float(str(row[col_ano]))) if col_ano else 0
            m = int(float(str(row[col_mes]))) if col_mes else 0
        except:
            continue
        if a != ano or m != mes:
            continue

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

def buscar_users_pipe():
    resp = req.get(f"{BASE_V1}/users", params={"api_token": API_KEY}, timeout=15)
    resp.raise_for_status()
    return {u["id"]: u["name"] for u in (resp.json().get("data") or [])}

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


def buscar_qual_ids():
    """Retorna {norm(label): str(id)} para o campo qualificador"""
    resp = req.get(f"{BASE_V1}/dealFields", params={"api_token": API_KEY}, timeout=15)
    resp.raise_for_status()
    for field in (resp.json().get("data") or []):
        if field.get("key") == CF_QUALIFICADOR:
            return {norm(opt.get("label", "")): str(opt.get("id")) for opt in (field.get("options") or [])}
    return {}

def buscar_activities_mes(mes, ano):
    todos, cursor = [], None
    mes_str = f"{ano}-{mes:02d}"
    while True:
        params = {"filter_id": FILTER_ACTIVITIES, "limit": 200}
        if cursor:
            params["cursor"] = cursor
        resp = req.get(f"{BASE_V2}/activities", params=params,
                       headers={"x-api-token": API_KEY}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        lote = data.get("data") or []
        for act in lote:
            if str(act.get("due_date", ""))[:7] == mes_str:
                todos.append(act)
        cursor = data.get("additional_data", {}).get("next_cursor")
        if not cursor or not lote:
            break
    return todos

def calcular_abril():
    hoje = date.today()
    mes, ano = hoje.month, hoje.year

    du_calc = du_mes_total(ano, mes)
    du_pass = du_passados(ano, mes)
    du_rest = du_restantes(ano, mes)

    colab_df   = buscar_colaboradores()
    metas      = buscar_metas_todas(ano, mes)
    users_pipe = buscar_users_pipe()
    deals      = buscar_deals_mes(mes, ano)

    uid_to_nome_norm = {uid: norm(name) for uid, name in users_pipe.items()}

    nome_to_subarea = {}
    for _, row in colab_df.iterrows():
        sub = str(row.get("Subarea", "")).strip()
        nome_to_subarea[norm(str(row.get("Nome", "")))] = sub

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

    closers_metas = [m for m in metas if m["meta_reu"] == 0 and m["meta_fin"] > 0]

    du_sheet = next((m["dias_uteis"] for m in closers_metas if m["dias_uteis"] > 0), 0)
    du_total = du_sheet if du_sheet > 0 else du_calc

    SQUADS_PERMITIDOS = {"sniper", "elite", "mgm"}

    squad_data = {}
    for m in closers_metas:
        nn  = m["nome_norm"]
        sub = nome_to_subarea.get(nn) or "Outros"
        if norm(sub) not in SQUADS_PERMITIDOS:
            continue
        ri  = closer_real.get(nn, {"valor": 0, "valor_multi": 0, "qtd": 0})
        if sub not in squad_data:
            squad_data[sub] = {"nome": sub, "meta": 0, "realizado": 0, "realizado_multi": 0, "qtd": 0}
        squad_data[sub]["meta"]            += m["meta_fin"]
        squad_data[sub]["realizado"]       += ri["valor"]
        squad_data[sub]["realizado_multi"] += ri["valor_multi"]
        squad_data[sub]["qtd"]             += ri["qtd"]

    def build_row(nome, meta, real, real_multi, qtd):
        mtd = safe_div(meta, du_total) * du_pass if du_total else 0
        return {
            "nome": nome, "meta": arred(meta),
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

    squads = [build_row(sd["nome"], sd["meta"], sd["realizado"], sd["realizado_multi"], sd["qtd"])
              for sd in squad_data.values()]

    t_meta  = sum(sd["meta"]            for sd in squad_data.values())
    t_real  = sum(sd["realizado"]       for sd in squad_data.values())
    t_multi = sum(sd["realizado_multi"] for sd in squad_data.values())
    t_qtd   = sum(sd["qtd"]            for sd in squad_data.values())


    # ── SDRs ──────────────────────────────────────────────────
    qual_ids   = buscar_qual_ids()
    activities = buscar_activities_mes(mes, ano)

    nome_norm_to_uid = {norm(name): uid for uid, name in users_pipe.items()}

    # Mapa reunião válida: deal_id -> CF_REUNIAO_VALID
    mapa_rv = {d["id"]: cf(d, CF_REUNIAO_VALID) for d in deals}

    def act_valida(act):
        if not (act.get("done") is True or act.get("status") == "done"):
            return False
        rv = mapa_rv.get(act.get("deal_id"))
        return rv is None or str(rv).strip() == "" or norm(str(rv)) == "sim"

    # Atividades por owner
    acts_by_owner = {}
    for act in activities:
        oid = str(act.get("owner_id", ""))
        acts_by_owner.setdefault(oid, []).append(act)

    sdrs_metas = [m for m in metas if m["meta_reu"] > 0 and m["meta_fin"] > 0]

    sdrs = []
    for m in sdrs_metas:
        nn       = m["nome_norm"]
        meta_reu = m["meta_reu"]
        meta_fin = m["meta_fin"] / 10  # mesmo ajuste dos closers

        uid     = nome_norm_to_uid.get(nn)
        uid_str = str(uid) if uid else ""
        acts_sdr = acts_by_owner.get(uid_str, [])

        validadas     = [a for a in acts_sdr if act_valida(a)]
        qtd_validadas = len(validadas)

        deveria_estar = arred(safe_div(meta_reu, du_total) * du_pass)
        faltam        = arred(deveria_estar - qtd_validadas)
        pct_reu       = arred(safe_div(qtd_validadas, meta_reu) * 100)

        qual_id    = qual_ids.get(nn)
        deals_sdr  = [d for d in deals if str(cf(d, CF_QUALIFICADOR)) == str(qual_id)] if qual_id else []
        qtd_ganhos = len(deals_sdr)
        valor_ganho      = sum(float(d.get("value") or 0) for d in deals_sdr)
        valor_ganho_multi = sum(float(cf(d, CF_MULTIPLICADOR) or 0) for d in deals_sdr)
        pct_ganhos  = arred(safe_div(valor_ganho_multi, meta_fin) * 100)
        ticket      = arred(safe_div(valor_ganho, qtd_ganhos)) if qtd_ganhos else 0
        pct_final   = arred((pct_reu + pct_ganhos) / 2)

        sdrs.append({
            "nome":               m["nome"],
            "meta_reuniao":       meta_reu,
            "meta_diaria":        arred(safe_div(meta_reu, du_total)),
            "validadas":          qtd_validadas,
            "deveria_estar":      deveria_estar,
            "faltam":             faltam,
            "pct_reu":            pct_reu,
            "meta_ganho":         arred(meta_fin),
            "qtd_ganhos":         qtd_ganhos,
            "valor_ganho":        arred(valor_ganho),
            "valor_ganho_multi":  arred(valor_ganho_multi),
            "pct_ganhos":         pct_ganhos,
            "ticket_medio":       ticket,
            "pct_final":          pct_final,
        })

    # Total SDR
    if sdrs:
        t_reu   = sum(s["meta_reuniao"] for s in sdrs)
        t_valid = sum(s["validadas"] for s in sdrs)
        t_meta_g = sum(s["meta_ganho"] for s in sdrs)
        t_valor  = sum(s["valor_ganho"] for s in sdrs)
        t_multi  = sum(s["valor_ganho_multi"] for s in sdrs)
        t_qtd    = sum(s["qtd_ganhos"] for s in sdrs)
        t_dev    = sum(s["deveria_estar"] for s in sdrs)
        t_pct_r  = arred(safe_div(t_valid, t_reu) * 100)
        t_pct_g  = arred(safe_div(t_multi, t_meta_g) * 100)
        sdrs.append({
            "nome": "TOTAL", "meta_reuniao": t_reu,
            "meta_diaria": arred(safe_div(t_reu, du_total)),
            "validadas": t_valid, "deveria_estar": arred(t_dev),
            "faltam": arred(t_dev - t_valid),
            "pct_reu": t_pct_r, "meta_ganho": arred(t_meta_g),
            "qtd_ganhos": t_qtd, "valor_ganho": arred(t_valor),
            "valor_ganho_multi": arred(t_multi),
            "pct_ganhos": t_pct_g, "ticket_medio": arred(safe_div(t_valor, t_qtd)) if t_qtd else 0,
            "pct_final": arred((t_pct_r + t_pct_g) / 2),
        })

    return {
        "periodo": {
            "mes": mes, "ano": ano,
            "du_total": du_total, "du_passados": du_pass, "du_restantes": du_rest,
            "atualizado_em": datetime.now().strftime("%d/%m/%Y %H:%M"),
        },
        "closers": {
            "squads": squads,
            "total":  build_row("MTD Faturamento", t_meta, t_real, t_multi, t_qtd),
            "meta_120": arred(t_meta * 1.2),
        },
        "sdrs": sdrs,
        "_debug": {
            "total_metas": len(metas),
            "closers_encontrados": len(closers_metas),
            "deals_mes": len(deals),
            "nomes_closers": [m["nome"] for m in closers_metas],
            "todos_abril": [{"nome": m["nome"], "meta_reu": m["meta_reu"], "meta_fin": m["meta_fin"]} for m in metas],
            "subareas": {m["nome"]: nome_to_subarea.get(m["nome_norm"], "NAO ENCONTRADO") for m in closers_metas},
        }
    }

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

def limpar_nans(obj):
    """Substitui NaN/inf por None recursivamente para JSON válido"""
    import math
    if isinstance(obj, dict):
        return {k: limpar_nans(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [limpar_nans(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj

@app.route("/api/abril")
def api_abril():
    if "nome" not in session:
        return jsonify({"erro": "Não autenticado"}), 401
    try:
        return jsonify(limpar_nans(calcular_abril()))
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
