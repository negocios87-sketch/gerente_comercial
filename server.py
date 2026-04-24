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

# ── CONFIG (variáveis de ambiente no Render) ─────────────────
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

# ── HELPERS ──────────────────────────────────────────────────
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
    return sum(
        1 for d in range(1, calendar.monthrange(ano, mes)[1] + 1)
        if date(ano, mes, d).weekday() < 5
    )

def du_passados(ano, mes):
    hoje = date.today()
    return max(sum(
        1 for d in range(1, min(hoje.day, calendar.monthrange(ano, mes)[1]) + 1)
        if date(ano, mes, d).weekday() < 5
    ), 1)

def du_restantes(ano, mes):
    hoje = date.today()
    ultimo = calendar.monthrange(ano, mes)[1]
    return sum(
        1 for d in range(hoje.day + 1, ultimo + 1)
        if date(ano, mes, d).weekday() < 5
    )

# ── SHEETS ──────────────────────────────────────────────────
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
        try: return float(str(v or "0").strip())
        except: return 0.0

    rows = []
    for _, row in df.iterrows():
        try:
            a = int(float(str(row.get("Ano", 0))))
            m = int(float(str(row.get("Mes", row.get("Mês", 0)))))
        except:
            continue
        if a != ano or m != mes:
            continue
        meta_reu = to_num(row.get("Meta de Reunioes", row.get("Meta de Reuniões", 0)))
        meta_fin = to_num(row.get("Meta Financeira", 0))
        nome_raw = str(row.get("Nome", "")).strip()
        rows.append({
            "nome": nome_raw,
            "nome_norm": norm(nome_raw),
            "meta_reu": meta_reu,
            "meta_fin": meta_fin,
        })
    return rows

# ── PIPEDRIVE ────────────────────────────────────────────────
def buscar_users_pipe():
    resp = req.get(f"{BASE_V1}/users", params={"api_token": API_KEY}, timeout=15)
    resp.raise_for_status()
    return {u["id"]: u["name"] for u in (resp.json().get("data") or [])}

def buscar_qual_ids():
    """Retorna {norm(label): str(id)} para o campo qualificador"""
    resp = req.get(f"{BASE_V1}/dealFields", params={"api_token": API_KEY}, timeout=15)
    resp.raise_for_status()
    for field in (resp.json().get("data") or []):
        if field.get("key") == CF_QUALIFICADOR:
            return {norm(opt.get("label", "")): str(opt.get("id")) for opt in (field.get("options") or [])}
    return {}

def buscar_deals_mes(mes, ano):
    """Busca deals ganhos no mês. Para paginação assim que achar deals de meses anteriores."""
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
            if wt == mes_str:
                todos.append(deal)
            elif wt < mes_str:
                found_older = True
        mais = data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection", False)
        if not mais or not lote or found_older:
            break
        start += 500
    return todos

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

# ── CÁLCULO PRINCIPAL ─────────────────────────────────────────
def calcular_abril():
    hoje = date.today()
    mes, ano = hoje.month, hoje.year

    du_total  = du_mes_total(ano, mes)
    du_pass   = du_passados(ano, mes)
    du_rest   = du_restantes(ano, mes)

    # Buscar tudo em paralelo seria ideal, mas keep it simple por ora
    colab_df   = buscar_colaboradores()
    metas      = buscar_metas_todas(ano, mes)
    users_pipe = buscar_users_pipe()
    qual_ids   = buscar_qual_ids()
    deals      = buscar_deals_mes(mes, ano)
    activities = buscar_activities_mes(mes, ano)

    # Mapas úteis
    uid_to_nome_norm = {uid: norm(name) for uid, name in users_pipe.items()}
    nome_norm_to_uid = {norm(name): uid for uid, name in users_pipe.items()}

    nome_to_subarea = {}
    for _, row in colab_df.iterrows():
        sub = str(row.get("Subarea", "")).strip()
        nome_to_subarea[norm(str(row.get("Nome", "")))] = sub

    # ── Reuniao valida (atividade) ───────────────────────────
    mapa_rv = {}
    for deal in deals:
        mapa_rv[deal["id"]] = cf(deal, CF_REUNIAO_VALID)

    def act_valida(act):
        done = act.get("done") is True or act.get("status") == "done"
        if not done: return False
        rv = mapa_rv.get(act.get("deal_id"))
        return rv is None or str(rv).strip() == "" or norm(str(rv)) == "sim"

    # ── Realizado por closer (owner_id) ──────────────────────
    closer_real = {}  # nome_norm -> {valor, valor_multi, qtd}
    for deal in deals:
        owner_nome = norm(get_owner_name(deal))
        if not owner_nome:
            oid = get_owner_id(deal)
            owner_nome = uid_to_nome_norm.get(oid, "")
        if not owner_nome:
            continue
        valor       = float(deal.get("value") or 0)
        valor_multi = float(cf(deal, CF_MULTIPLICADOR) or 0)
        if owner_nome not in closer_real:
            closer_real[owner_nome] = {"valor": 0, "valor_multi": 0, "qtd": 0}
        closer_real[owner_nome]["valor"]       += valor
        closer_real[owner_nome]["valor_multi"] += valor_multi
        closer_real[owner_nome]["qtd"]         += 1

    # ── Atividades por owner ──────────────────────────────────
    acts_by_owner = {}  # str(uid) -> [acts]
    for act in activities:
        oid = str(act.get("owner_id", ""))
        acts_by_owner.setdefault(oid, []).append(act)

    # ── Closers: agrupa por Squad ─────────────────────────────
    closers_metas = [m for m in metas if m["meta_reu"] == 0 and m["meta_fin"] > 0]

    squad_data = {}  # subarea -> acumuladores
    for m in closers_metas:
        nn     = m["nome_norm"]
        sub    = nome_to_subarea.get(nn, "Outros")
        real_i = closer_real.get(nn, {"valor": 0, "valor_multi": 0, "qtd": 0})

        if sub not in squad_data:
            squad_data[sub] = {"nome": sub, "meta": 0, "realizado": 0, "realizado_multi": 0, "qtd": 0}
        squad_data[sub]["meta"]            += m["meta_fin"]
        squad_data[sub]["realizado"]       += real_i["valor"]
        squad_data[sub]["realizado_multi"] += real_i["valor_multi"]
        squad_data[sub]["qtd"]             += real_i["qtd"]

    def build_squad_row(nome, meta, real, real_multi, qtd):
        mtd           = safe_div(meta, du_total) * du_pass
        ticket        = arred(safe_div(real, qtd)) if qtd else 0
        ticket_multi  = arred(safe_div(real_multi, qtd)) if qtd else 0
        return {
            "nome":                      nome,
            "meta":                      arred(meta),
            "realizado":                 arred(real),
            "pct_atingido":              arred(safe_div(real, meta) * 100),
            "mtd":                       arred(mtd),
            "deficit_mtd":               arred(mtd - real),
            "pct_mtd":                   arred(safe_div(real, mtd) * 100),
            "deficit_meta":              arred(meta - real),
            "meta_dia_100":              arred(safe_div(meta - real, du_rest)) if du_rest else 0,
            "realizado_multi":           arred(real_multi),
            "pct_atingido_multi":        arred(safe_div(real_multi, meta) * 100),
            "deficit_meta_multi":        arred(meta - real_multi),
            "meta_dia_100_multi":        arred(safe_div(meta - real_multi, du_rest)) if du_rest else 0,
            "qtd_ganhos":                qtd,
            "ticket_medio":              ticket,
            "ticket_medio_multi":        ticket_multi,
        }

    squads = []
    tot_meta = tot_real = tot_real_m = tot_qtd = 0
    for sub, sd in squad_data.items():
        squads.append(build_squad_row(sd["nome"], sd["meta"], sd["realizado"], sd["realizado_multi"], sd["qtd"]))
        tot_meta   += sd["meta"]
        tot_real   += sd["realizado"]
        tot_real_m += sd["realizado_multi"]
        tot_qtd    += sd["qtd"]

    total_row = build_squad_row("MTD Faturamento", tot_meta, tot_real, tot_real_m, tot_qtd)
    meta_120  = arred(tot_meta * 1.2)

    # ── Geral: individual closer % ────────────────────────────
    geral = []
    for m in closers_metas:
        nn      = m["nome_norm"]
        sub     = nome_to_subarea.get(nn, "")
        real_i  = closer_real.get(nn, {"valor_multi": 0})
        pct     = arred(safe_div(real_i["valor_multi"], m["meta_fin"]) * 100) if m["meta_fin"] else None
        if pct is not None:
            geral.append({"nome": m["nome"], "time": sub, "pct": pct})
    geral.sort(key=lambda x: -x["pct"])

    # ── SDRs ──────────────────────────────────────────────────
    sdrs_metas = [m for m in metas if m["meta_reu"] > 0]
    sdrs = []

    for m in sdrs_metas:
        nn       = m["nome_norm"]
        meta_reu = m["meta_reu"]
        meta_fin = m["meta_fin"]

        uid      = nome_norm_to_uid.get(nn)
        uid_str  = str(uid) if uid else ""
        acts_sdr = acts_by_owner.get(uid_str, [])

        realizadas    = [a for a in acts_sdr if act_valida(a)]
        qtd_real      = len(realizadas)
        deveria_estar = arred(safe_div(meta_reu, du_total) * du_pass)
        faltam        = arred(deveria_estar - qtd_real)
        pct_reu       = arred(safe_div(qtd_real, meta_reu) * 100)

        qual_id   = qual_ids.get(nn)
        deals_sdr = [d for d in deals if str(cf(d, CF_QUALIFICADOR)) == str(qual_id)] if qual_id else []
        qtd_ganhos  = len(deals_sdr)
        valor_ganho = sum(float(d.get("value") or 0) for d in deals_sdr)
        pct_ganhos  = arred(safe_div(valor_ganho, meta_fin) * 100) if meta_fin else 0
        ticket      = arred(safe_div(valor_ganho, qtd_ganhos)) if qtd_ganhos else 0
        media_final = arred((pct_reu + pct_ganhos) / 2)

        sdrs.append({
            "nome":           m["nome"],
            "meta_reuniao":   meta_reu,
            "meta_diaria":    arred(safe_div(meta_reu, du_total)),
            "validadas":      qtd_real,
            "deveria_estar":  deveria_estar,
            "faltam":         faltam,
            "pct_atingido":   pct_reu,
            "meta_ganho":     meta_fin,
            "qtd_ganhos":     qtd_ganhos,
            "valor_ganho":    arred(valor_ganho),
            "pct_ganhos":     pct_ganhos,
            "ticket_medio":   ticket,
            "media_final":    media_final,
        })

    # Linha total SDR
    if sdrs:
        t_reu  = sum(s["meta_reuniao"] for s in sdrs)
        t_real = sum(s["validadas"] for s in sdrs)
        t_meta_g = sum(s["meta_ganho"] for s in sdrs)
        t_valor  = sum(s["valor_ganho"] for s in sdrs)
        t_qtd_g  = sum(s["qtd_ganhos"] for s in sdrs)
        sdrs.append({
            "nome":           "TOTAL",
            "meta_reuniao":   t_reu,
            "meta_diaria":    arred(safe_div(t_reu, du_total)),
            "validadas":      t_real,
            "deveria_estar":  arred(safe_div(t_reu, du_total) * du_pass),
            "faltam":         arred(arred(safe_div(t_reu, du_total) * du_pass) - t_real),
            "pct_atingido":   arred(safe_div(t_real, t_reu) * 100),
            "meta_ganho":     t_meta_g,
            "qtd_ganhos":     t_qtd_g,
            "valor_ganho":    arred(t_valor),
            "pct_ganhos":     arred(safe_div(t_valor, t_meta_g) * 100),
            "ticket_medio":   arred(safe_div(t_valor, t_qtd_g)) if t_qtd_g else 0,
            "media_final":    arred((arred(safe_div(t_real, t_reu) * 100) + arred(safe_div(t_valor, t_meta_g) * 100)) / 2),
        })

    return {
        "periodo": {
            "mes": mes, "ano": ano,
            "du_total": du_total, "du_passados": du_pass, "du_restantes": du_rest,
            "atualizado_em": datetime.now().strftime("%d/%m/%Y %H:%M"),
        },
        "closers": {"squads": squads, "total": total_row, "meta_120": meta_120},
        "sdrs": sdrs,
        "geral": geral,
    }

# ── ROTAS ────────────────────────────────────────────────────
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
        return jsonify(calcular_abril())
    except Exception as e:
        import traceback
        return jsonify({"erro": str(e), "trace": traceback.format_exc()}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
