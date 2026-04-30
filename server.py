"""
Board Academy — Dashboard SDR
Servidor Python + Flask para Render

Variáveis de ambiente necessárias no Render:
    PIPEDRIVE_KEY = sua_chave_aqui
    SECRET_KEY    = qualquer_string_aleatoria
"""

from flask import Flask, jsonify, request, session, redirect, send_from_directory
import requests as req
import pandas as pd
import os
import unicodedata
from datetime import date, datetime, timedelta
from io import StringIO

app = Flask(__name__, static_folder="static")
app.secret_key = os.getenv("SECRET_KEY", "boardacademy2026")

# ── CONFIG ──────────────────────────────────────────────────
API_KEY  = os.getenv("PIPEDRIVE_KEY")
BASE_V1  = "https://boardacademy.pipedrive.com/api/v1"
BASE_V2  = "https://boardacademy.pipedrive.com/api/v2"

GITHUB_FOTOS = "https://raw.githubusercontent.com/negocios87-sketch/fotos_time_comercial/main"

URL_COLAB = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSvwO3Ag2f2cbkVgR1pJZp6fANQcbualGKlAG50fmOljuEGKZ1gJBbSAjRdO3SomXUEVQOWnTvlfHRd/pub?gid=1782440078&single=true&output=csv"
URL_METAS = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSvwO3Ag2f2cbkVgR1pJZp6fANQcbualGKlAG50fmOljuEGKZ1gJBbSAjRdO3SomXUEVQOWnTvlfHRd/pub?gid=0&single=true&output=csv"
URL_OTE   = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSvwO3Ag2f2cbkVgR1pJZp6fANQcbualGKlAG50fmOljuEGKZ1gJBbSAjRdO3SomXUEVQOWnTvlfHRd/pub?gid=569278086&single=true&output=csv"
URL_USERS = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSvwO3Ag2f2cbkVgR1pJZp6fANQcbualGKlAG50fmOljuEGKZ1gJBbSAjRdO3SomXUEVQOWnTvlfHRd/pub?gid=160245570&single=true&output=csv"

FILTER_DEALS      = 74674
FILTER_ACTIVITIES = 1310451

CF_MULTIPLICADOR = "7e0e43c2734751f77be292a72527f638a850ad50"
CF_QUALIFICADOR  = "a6f13cc27c8d041f3af4091283ce0d4fe0913875"
CF_REUNIAO_VALID = "7299bf170c5deab9b4fd8c2275f55faf51984dea"

# IDs do campo Reunião Validada?
RV_SIM     = "411"
RV_NAO     = "412"
RV_NOSHOW  = "481"

TIMES_ESCOPO = ["elite", "sniper", "atlantis", "mgm"]

# ── HELPERS ─────────────────────────────────────────────────
def norm(s):
    if not s:
        return ""
    s = str(s).strip().lower()
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode()

def arred(v):
    try:
        return round(float(v), 2)
    except:
        return 0.0

def acelerador(pct):
    if pct >= 120: return 1.5
    if pct >= 100: return 1.0
    if pct >= 86:  return 0.7
    if pct >= 60:  return 0.5
    return 0.0

def dias_uteis_passados():
    hoje = date.today()
    count = 0
    d = date(hoje.year, hoje.month, 1)
    while d <= hoje:
        if d.weekday() < 5:
            count += 1
        if d == hoje:
            break
        try:
            d = date(d.year, d.month, d.day + 1)
        except:
            break
    return max(count, 1)

def ajustar_hora(hora_str):
    if not hora_str:
        return None
    try:
        h, m = hora_str[:5].split(":")
        dt = datetime(2000, 1, 1, int(h), int(m)) - timedelta(hours=3)
        return dt.strftime("%H:%M")
    except:
        return hora_str

def url_foto(nome):
    """Tenta JPG, JPEG e PNG no GitHub"""
    base = f"{GITHUB_FOTOS}/{nome}"
    for ext in ["jpg", "jpeg", "png", "JPG", "JPEG", "PNG"]:
        url = f"{base}.{ext}"
        try:
            r = req.head(url, timeout=5)
            if r.status_code == 200:
                return url
        except:
            pass
    return None

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

def buscar_colaborador(nome):
    df = ler_sheet(URL_COLAB)
    df.columns = [c.strip() for c in df.columns]
    for _, row in df.iterrows():
        subarea = str(row.get("Subarea", ""))
        status  = str(row.get("Status (Euqipe Comercial)", ""))
        if (norm(str(row.get("Nome", ""))) == norm(nome) and
            norm(status) == "ativo" and
            norm(subarea) in TIMES_ESCOPO):
            return {
                "nome":  str(row.get("Nome", "")),
                "time":  subarea,
                "cargo": str(row.get("Cargo", "")),
            }
    return None

def buscar_metas(nome):
    df = ler_sheet(URL_METAS)
    df.columns = [c.strip() for c in df.columns]
    hoje = date.today()

    def limpar_numero(val):
        try:
            return float(str(val or "0").strip())
        except:
            return 0.0

    for _, row in df.iterrows():
        try:
            ano = int(float(str(row.get("Ano", 0))))
            mes = int(float(str(row.get("Mes", row.get("Mês", 0)))))
        except:
            continue
        if norm(row.get("Nome")) == norm(nome) and ano == hoje.year and mes == hoje.month:
            ramp_str = str(row.get("% de Rampagem", "1") or "1").replace("%","").strip()
            ramp_val = float(ramp_str)
            ramp = ramp_val / 100 if ramp_val > 1 else ramp_val
            return {
                "meta_reu":   limpar_numero(row.get("Meta de Reunioes", row.get("Meta de Reuniões", 0))),
                "meta_fin":   limpar_numero(row.get("Meta Financeira", 0)),
                "rampagem":   ramp,
                "dias_uteis": int(float(row.get("Dias Uteis", 20) or 20)),
            }
    return None

def buscar_ote(cargo):
    df = ler_sheet(URL_OTE)
    df.columns = [c.strip() for c in df.columns]
    for _, row in df.iterrows():
        nivel = str(row.get("Nivel", row.get("Nível", "")))
        if norm(nivel) == norm(cargo):
            return {
                "total":    float(row.get("ote_total", 0) or 0),
                "fixo":     float(row.get("fixo", 0) or 0),
                "variavel": float(row.get("variavel", 0) or 0),
            }
    return None

# ── PIPEDRIVE ────────────────────────────────────────────────
def buscar_qualificador_id(nome):
    resp = req.get(f"{BASE_V1}/dealFields",
        params={"api_token": API_KEY}, timeout=15)
    resp.raise_for_status()
    fields = resp.json().get("data") or []
    for field in fields:
        if field.get("key") == CF_QUALIFICADOR:
            for opt in (field.get("options") or []):
                if norm(opt.get("label","")) == norm(nome):
                    return str(opt.get("id"))
    return None

def buscar_users():
    resp = req.get(f"{BASE_V1}/users",
        params={"api_token": API_KEY}, timeout=15)
    resp.raise_for_status()
    mapa = {}
    for u in (resp.json().get("data") or []):
        mapa[u["id"]] = u["name"]
    return mapa

def encontrar_user_id(users, nome):
    for uid, uname in users.items():
        if norm(uname) == norm(nome):
            return uid
    return None

def buscar_deals():
    todos, start = [], 0
    while True:
        resp = req.get(f"{BASE_V1}/deals", params={
            "filter_id": FILTER_DEALS,
            "limit": 500,
            "start": start,
            "api_token": API_KEY
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        lote = data.get("data") or []
        todos.extend(lote)
        mais = data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection", False)
        if not mais or not lote:
            break
        start += 500
    return todos

def buscar_activities():
    todos, cursor = [], None
    while True:
        params = {"filter_id": FILTER_ACTIVITIES, "limit": 200}
        if cursor:
            params["cursor"] = cursor
        resp = req.get(f"{BASE_V2}/activities",
            params=params,
            headers={"x-api-token": API_KEY},
            timeout=30)
        resp.raise_for_status()
        data   = resp.json()
        lote   = data.get("data") or []
        todos.extend(lote)
        cursor = data.get("additional_data", {}).get("next_cursor")
        if not cursor or not lote:
            break
    return todos

def cf(deal, key):
    val = deal.get(key)
    if val is None:
        return None
    if isinstance(val, dict):
        return val.get("value") or val.get("label")
    return val

# ── CÁLCULO ──────────────────────────────────────────────────
def calcular(nome, user_id, qualificador_id, colaborador, metas, ote, deals, activities):

    deals_ganhos = [
        d for d in deals
        if d.get("status") == "won" and str(cf(d, CF_QUALIFICADOR)) == str(qualificador_id)
    ]

    valor_bruto = sum(float(d.get("value") or 0) for d in deals_ganhos)
    valor_multi = sum(float(cf(d, CF_MULTIPLICADOR) or 0) for d in deals_ganhos)

    mapa_rv = {d["id"]: cf(d, CF_REUNIAO_VALID) for d in deals}

    hoje      = date.today()
    mes_atual = hoje.strftime("%Y-%m")

    acts_sdr = [
        a for a in activities
        if str(a.get("owner_id")) == str(user_id)
        and str(a.get("due_date", ""))[:7] == mes_atual
    ]

    def rv_eh_valida(rv):
        if rv is None or str(rv).strip() == "":
            return True
        s = str(rv)
        return s == RV_SIM or norm(s) == "sim"

    def rv_eh_desq(rv):
        if rv is None: return False
        s = str(rv)
        return s == RV_NAO or norm(s) == "nao"

    def valida(a):
        if not (a.get("done") == True or a.get("status") == "done"):
            return False
        rv = mapa_rv.get(a.get("deal_id"))
        return rv_eh_valida(rv)

    reu_realizadas = [a for a in acts_sdr if valida(a)]
    reu_desq       = [a for a in acts_sdr
                      if (a.get("done") == True or a.get("status") == "done")
                      and rv_eh_desq(mapa_rv.get(a.get("deal_id")))]

    qtd_agendadas  = len(acts_sdr)
    qtd_realizadas = len(reu_realizadas)

    pct_reu = (qtd_realizadas / metas["meta_reu"]) if metas["meta_reu"] > 0 else 0
    pct_fin = (valor_multi / metas["meta_fin"])    if metas["meta_fin"] > 0 else 0
    ating   = ((pct_reu + pct_fin) / 2) * 100

    acel     = acelerador(ating)
    comissao = ote["variavel"] * (ating / 100) * metas["rampagem"] * acel

    dp = dias_uteis_passados()
    du = metas["dias_uteis"]
    proj_fin      = (valor_multi / dp) * du
    proj_reu      = (qtd_realizadas / dp) * du
    pct_proj_fin  = (proj_fin / metas["meta_fin"] * 100) if metas["meta_fin"] > 0 else 0
    pct_proj_reu  = (proj_reu / metas["meta_reu"] * 100)  if metas["meta_reu"] > 0 else 0
    ating_proj    = (pct_proj_fin + pct_proj_reu) / 2
    acel_proj     = acelerador(ating_proj)
    comissao_proj = ote["variavel"] * (ating_proj / 100) * metas["rampagem"] * acel_proj

    def serie_acts(lista):
        mapa = {}
        for a in lista:
            dt = str(a.get("due_date", ""))[:10]
            if dt:
                mapa[dt] = mapa.get(dt, 0) + 1
        return [{"data": k, "qtd": v} for k, v in sorted(mapa.items())]

    def serie_deals(lista):
        mapa = {}
        for d in lista:
            dt = str(d.get("won_time", ""))[:10]
            if dt:
                v = float(cf(d, CF_MULTIPLICADOR) or 0)
                mapa[dt] = mapa.get(dt, 0) + v
        return [{"data": k, "valor": v} for k, v in sorted(mapa.items())]

    proximas = sorted(
        [a for a in acts_sdr if not (a.get("done") or a.get("status") == "done")],
        key=lambda a: (a.get("due_date", ""), a.get("due_time", "") or "")
    )[:10]

    foto = url_foto(nome)

    return {
        "colaborador": {**colaborador, "foto": foto},
        "metas":       metas,
        "ote":         ote,
        "kpis": {
            "reunioesAgendadas":  qtd_agendadas,
            "reunioesRealizadas": qtd_realizadas,
            "pctMetaReu":         arred(pct_reu * 100),
            "valorBruto":         arred(valor_bruto),
            "valorMultiplicador": arred(valor_multi),
            "pctMetaFin":         arred(pct_fin * 100),
            "pctDesqualificacao": arred(len(reu_desq) / qtd_realizadas * 100) if qtd_realizadas else 0,
        },
        "comissao": {
            "atingimento":   arred(ating),
            "acelerador":    acel,
            "estimada":      arred(comissao),
            "comissionando": ating >= 60,
            "projecao": {
                "valorFin":    arred(proj_fin),
                "qtdReu":      arred(proj_reu),
                "atingimento": arred(ating_proj),
                "estimada":    arred(comissao_proj),
            }
        },
        "graficos": {
            "serieAgendadas":  serie_acts(acts_sdr),
            "serieRealizadas": serie_acts(reu_realizadas),
            "serieValores":    serie_deals(deals_ganhos),
        },
        "tabelas": {
            "proximasReunioes": [{"deal_id": a.get("deal_id"), "nome": a.get("subject", ""), "data": a.get("due_date"), "hora": ajustar_hora(a.get("due_time"))} for a in proximas],
            "reunioesGanhas":   [{"deal_id": d["id"], "nome": d.get("title"), "data_ganho": str(d.get("won_time", ""))[:10], "valor_bruto": float(d.get("value") or 0), "valor_multi": float(cf(d, CF_MULTIPLICADOR) or 0)} for d in deals_ganhos[:10]],
            "reunioesDesq": [{
                "deal_id":     a.get("deal_id"),
                "proprietario": next((d.get("owner_name") or (d.get("user_id") or {}).get("name","") for d in deals if d["id"] == a.get("deal_id")), "--")
            } for a in reu_desq[:10]],
        },
        "atualizadoEm": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
    }

# ── ROTAS ────────────────────────────────────────────────────
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        usuario = request.form.get("usuario", "").strip()
        senha   = request.form.get("senha", "").strip()
        nome    = buscar_usuario(usuario, senha)
        if nome:
            session["nome"] = nome
            return redirect("/dashboard")
        return send_from_directory(".", "login.html"), 401
    return send_from_directory(".", "login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")

@app.route("/dashboard")
def dashboard():
    if "nome" not in session:
        return redirect("/login")
    return send_from_directory(".", "dashboard.html")

@app.route("/api/sdr")
def api_sdr():
    if "nome" not in session:
        return jsonify({"erro": "Não autenticado"}), 401

    nome = session["nome"]
    try:
        colaborador = buscar_colaborador(nome)
        if not colaborador:
            return jsonify({"erro": f"'{nome}' nao encontrado ou inativo"}), 404

        metas = buscar_metas(nome)
        if not metas:
            return jsonify({"erro": f"Metas de '{nome}' nao encontradas para o mes atual"}), 404

        ote = buscar_ote(colaborador["cargo"])
        if not ote:
            return jsonify({"erro": f"OTE nao encontrado para cargo '{colaborador['cargo']}'"}), 404

        users           = buscar_users()
        user_id         = encontrar_user_id(users, nome)
        if not user_id:
            return jsonify({"erro": f"Usuario '{nome}' nao encontrado no Pipedrive"}), 404

        deals           = buscar_deals()
        activities      = buscar_activities()
        qualificador_id = buscar_qualificador_id(nome)

        resultado = calcular(nome, user_id, qualificador_id, colaborador, metas, ote, deals, activities)
        return jsonify(resultado)

    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route("/")
def index():
    return redirect("/login")

# ── MAIN ─────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5050))
    print(f"\n Board Academy — http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
