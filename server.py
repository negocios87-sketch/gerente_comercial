"""
Board Academy — Gerente Comercial (Forecast Dashboard)
Deploy: Vercel. Regras de negócio: biblioteca crn (negocios87-sketch/CRN, branch main).

Nenhuma regra é calculada aqui: o painel busca, chama as funções da crn e monta o JSON do front.
Se uma conta não bate, a mudança é na CRN, não neste arquivo.
"""
import os

# ── AMBIENTE ──────────────────────────────────────────────────
# Nada muda na Vercel: o token continua em PIPE_API_KEY e é repassado para o nome que a crn lê.
if os.environ.get("PIPE_API_KEY"):
    os.environ.setdefault("PIPEDRIVE_TOKEN", os.environ["PIPE_API_KEY"])
# Links das abas (provisório, decidido em 21/10/2026: resolver depois tirando do código).
_PLANILHA = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSvwO3Ag2f2cbkVgR1pJZp6fANQcbualGKlAG50fmOljuEGKZ1gJBbSAjRdO3SomXUEVQOWnTvlfHRd/pub?single=true&output=csv&gid="
os.environ.setdefault("URL_COLAB",    _PLANILHA + "1782440078")
os.environ.setdefault("URL_METAS",    _PLANILHA + "0")
os.environ.setdefault("URL_USERS",    _PLANILHA + "160245570")
os.environ.setdefault("URL_FERIADOS", _PLANILHA + "1010928978")

import calendar
import csv
import io
import math
import time
import traceback
from collections import defaultdict
from datetime import date, timedelta

from flask import Flask, Response, jsonify, redirect, render_template, request, session


# ── CRN: baixada do GitHub quando o painel liga ───────────────
# A Vercel não repassa o token para o instalador do build, então a CRN não entra pelo
# requirements.txt. O painel baixa a versão atual da main (com o token, que em tempo de
# execução funciona) e carrega. Cada vez que o painel liga, pega as regras mais recentes.
# No computador, se a crn já estiver instalada (instalar.bat), usa a instalada.
def _carregar_crn():
    import importlib.util
    import shutil
    import sys
    import tarfile
    import tempfile
    if importlib.util.find_spec("crn") is not None:
        return None
    import requests
    token = (os.environ.get("CRN_GITHUB_TOKEN") or os.environ.get("GITHUB_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("CRN_GITHUB_TOKEN não está nas Environment Variables da Vercel.")
    r = requests.get("https://api.github.com/repos/negocios87-sketch/CRN/tarball/main",
                     headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
                     timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Não consegui baixar a CRN do GitHub (HTTP {r.status_code}). Confira o CRN_GITHUB_TOKEN.")
    base = tempfile.mkdtemp(prefix="crn_")
    with tarfile.open(fileobj=io.BytesIO(r.content), mode="r:gz") as tar:
        tar.extractall(base, filter="data")
    raiz = os.path.join(base, os.listdir(base)[0])          # negocios87-sketch-CRN-<commit>
    destino = os.path.join(base, "pacote", "crn")
    shutil.copytree(os.path.join(raiz, "lib"), destino)     # mesmo mapa do pyproject da CRN
    shutil.copytree(os.path.join(raiz, "config"), os.path.join(destino, "config"))
    sys.path.insert(0, os.path.join(base, "pacote"))
    return os.path.basename(raiz).rsplit("-", 1)[-1][:7]   # commit que entrou

_COMMIT_CRN = _carregar_crn()

import crn
if _COMMIT_CRN:
    crn.versao = lambda: _COMMIT_CRN                        # carimbo mostra o commit carregado
from crn.base import fontes, pipedrive
from crn.base.config import campo, config, filtro
from crn.base.fontes import FonteInvalida
from crn.base.fuso import agora_brasil, atividade_dia, data_brasil, data_local, hoje_brasil
from crn.base.texto import cf, dono_id, norm
from crn.regras import (a1_resultado_squad as a1, as3_meta_ascensao as as3, f1_forecast_faixas as f1,
                        f2_forecast_media as f2, f3_forecast_perda as f3, f4_conversao_forecast as f4,
                        h1_hierarquia as h1, lc1_licenciado as lc1, m1_ganho_closer as m1,
                        m2_faturamento_closer as m2, m3_atingimento_closer as m3, m4_ganho_sdr as m4,
                        m5_faturamento_sdr as m5, m6_reunioes_sdr as m6, m7_pct_final_sdr as m7,
                        rf1_referidos as rf1, t1_mtd_closer as t1)

app = Flask(__name__, template_folder='.')
app.secret_key = os.environ.get("SECRET_KEY", "boardacademy2026secret")

SUPERUSERS_RAW = os.environ.get("SUPERUSERS", "farias souza")
MASTERS_RAW    = os.environ.get("MASTERS", "rodrigo leira,matheus paz,farias souza")
DENISE_NORM    = "denise mussolin"

# ── RECORTES DO PAINEL (escolha do painel, não regra; vão no carimbo) ─────────
SQUADS_SEM_SDR   = {"latam", "vanguard"}                      # não mostram líder na tabela de SDR
EXCLUIR_CARDS    = {"inteligencia comercial", "int. comercial"}
EXCLUIR_GERAL    = {"zenite", "licenciados", "inteligencia comercial", "int. comercial"}  # Total Geral (decidido 21/10/2026)
# Consolidados da Denise e "Geral": PROVISÓRIOS até a FT1 (frentes) sair de Decisão.
DENISE_SQUADS    = {"sniper", "elite", "mgm", "olympus", "ascensao"}
DENISE_FC        = {"sniper", "elite", "olympus", "mgm"}
GERAL_FC         = {"sniper", "elite", "olympus", "mgm", "latam", "vanguard", "zenite"}
LICENCIADOS      = "Licenciados"
CACHE_PIPEDRIVE  = 120   # segundos: a aba do mês e o Overview usam a mesma busca

# Confere se a CRN instalada tem o que este painel usa. Se a Vercel reaproveitou uma CRN antiga
# do cache, o painel avisa em vez de cair com FUNCTION_INVOCATION_FAILED.
CRN_DESATUALIZADA = None
try:
    import inspect
    SQUAD_ASCENSAO = norm(config()["AS3_meta_ascensao"]["squad"])
    FORA_FORECAST  = {norm(s) for s in config()["squads"]["fora_do_forecast"]}
    if "tipos" not in inspect.signature(pipedrive.atividades_do_periodo).parameters:
        raise KeyError("atividades_do_periodo(tipos=...)")
except KeyError as falta:
    SQUAD_ASCENSAO, FORA_FORECAST = "", set()
    CRN_DESATUALIZADA = (f"CRN instalada desatualizada (versão {crn.versao()}, falta {falta}). "
                         "Na Vercel: Deployments → ⋯ → Redeploy, desmarcando 'Use existing Build Cache'.")

@app.before_request
def _checar_crn():
    if CRN_DESATUALIZADA:
        if request.path.startswith("/api/"):
            return jsonify({"erro": CRN_DESATUALIZADA}), 500
        return f"<p style='font-family:sans-serif;padding:24px'>{CRN_DESATUALIZADA}</p>", 500

# ── HELPERS ───────────────────────────────────────────────────
def arred(v):
    try:
        f = float(v)
        return 0.0 if math.isnan(f) or math.isinf(f) else round(f, 2)
    except (TypeError, ValueError):
        return 0.0

def safe_div(a, b):
    try: return float(a) / float(b) if b else 0.0
    except (TypeError, ValueError): return 0.0

def limpar_nans(obj):
    if isinstance(obj, dict): return {k: limpar_nans(v) for k, v in obj.items() if not str(k).startswith("_")}
    if isinstance(obj, list): return [limpar_nans(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)): return None
    return obj

def agora_str():
    return agora_brasil().strftime("%d/%m/%Y %H:%M")

def carimbo(texto):
    return f"CRN {crn.versao()} · {texto} · Atualizado: {agora_str()}"

_memo = {}
def memo(chave, fn, ttl=CACHE_PIPEDRIVE):
    agora = time.time()
    if chave in _memo and agora - _memo[chave][0] < ttl:
        return _memo[chave][1]
    valor = fn()
    _memo[chave] = (agora, valor)
    return valor

def limites_mes(ano, mes):
    return date(ano, mes, 1), date(ano, mes, calendar.monthrange(ano, mes)[1])

def eh_lic(squad):
    return (squad or "").upper().startswith("LIC")

def erro_json(e):
    if isinstance(e, FonteInvalida):
        return jsonify({"erro": str(e)}), 500
    return jsonify({"erro": str(e), "trace": traceback.format_exc()}), 500

# ── LOGIN ─────────────────────────────────────────────────────
def buscar_usuario(usuario, senha):
    for row in fontes.ler_csv("URL_USERS"):
        r = {k.strip().lower(): v for k, v in row.items()}
        if norm(r.get("usuario", "")) == norm(usuario) and str(r.get("senha", "")).strip() == str(senha).strip():
            return str(r.get("usuario", ""))
    return None

def is_master(nome_sess):
    return norm(nome_sess) in {norm(u.strip()) for u in MASTERS_RAW.split(",")}

def is_superuser(nome_sess):
    return norm(nome_sess) in {norm(u.strip()) for u in SUPERUSERS_RAW.split(",")}

def head_filter_da_sessao(nome_sess, colab):
    """Quem vê o quê (H1 identifica head/líder; o recorte de visão é do painel)."""
    nn = norm(nome_sess)
    if is_superuser(nome_sess): return None
    if nn == DENISE_NORM:       return "__denise__"
    if any(p["head_norm"] == nn for p in colab): return nome_sess
    if any(p["lider_norm"] == nn and p["squad"] for p in colab): return f"__lider__:{nome_sess}"
    return "__none__"

# ── DADOS DO MÊS (uma busca, usada por aba do mês, Overview, Ranking e CSV) ───
def _carregar_mes(ano, mes):
    ini, fim = limites_mes(ano, mes)
    feriados = fontes.carregar_feriados()
    du       = fontes.dias_uteis_periodo(ano, mes, feriados, hoje_brasil())   # A2
    colab    = fontes.carregar_colab(ano, mes)                                 # erro se o mês não tem Colab
    metas    = fontes.carregar_metas(ano, mes)
    usuarios = pipedrive.usuarios()
    funis    = pipedrive.funis()
    opc_qual = pipedrive.opcoes_campo(campo("qualificador"))

    # Ganhos (M1): só o período, sem filtro salvo; R4/R3/G1 separados pela crn
    ganhos_brutos = pipedrive.ganhos_do_periodo(ini, fim)
    sep           = m1.separar_ganhos(ganhos_brutos, ano, mes, usuarios)
    por_closer    = m1.ganhos_por_closer(sep["validos"], colab, funis, usuarios)
    por_sdr       = m4.ganhos_por_sdr(sep["validos"], opc_qual)                # M4

    # Reuniões (M6): só reuniões feitas do período, deals pelos ids
    atividades  = pipedrive.atividades_do_periodo(ini, fim, tipos=["meeting"], done=True)
    deals_reu   = pipedrive.deals_por_ids([a.get("deal_id") for a in atividades])
    validados, donos = m6.mapas_dos_deals(deals_reu)
    nn_para_uid = {norm(n): uid for uid, n in usuarios.items()}
    sdr_ids     = {nn_para_uid[m["nome_norm"]] for m in metas if m["papel"] == "sdr" and m["nome_norm"] in nn_para_uid}
    lider_ids   = {nn_para_uid[p["nome_norm"]] for p in colab
                   if h1.nivel(p, colab) != "membro" and p["nome_norm"] in nn_para_uid}
    reunioes    = m6.reunioes_validadas(atividades, ini, fim, deals_validados=validados, dono_do_deal=donos,
                                        sdr_ids=sdr_ids, lideranca_ids=lider_ids)

    # Referidos (RF1): o filtro 1562285 ainda é a definição (pendência na CRN)
    refs_por_uid = rf1.referidos_do_mes(pipedrive.deals_do_filtro(filtro("referidos")), ano, mes)
    referidos    = defaultdict(int)
    for uid, lst in refs_por_uid.items():
        referidos[norm(usuarios.get(uid, ""))] += len(lst)

    return {
        "ano": ano, "mes": mes, "ini": ini, "fim": fim, "feriados": feriados, "du": du,
        "colab": colab, "metas": metas, "usuarios": usuarios, "funis": funis,
        "ganhos_brutos": ganhos_brutos, "sep": sep, "por_closer": por_closer, "por_sdr": por_sdr,
        "reunioes": reunioes, "titulos": {d["id"]: d.get("title", "") for d in deals_reu},
        "nn_para_uid": nn_para_uid, "referidos": referidos,
    }

def carregar_mes(ano, mes):
    return memo(("mes", ano, mes), lambda: _carregar_mes(ano, mes))

def mes_ano_da_req():
    hoje = hoje_brasil()
    return (request.args.get("mes", type=int) or hoje.month), (request.args.get("ano", type=int) or hoje.year)

# ── VISIBILIDADE ──────────────────────────────────────────────
def montar_visibilidade(head_filter, colab):
    sq_de   = {p["nome_norm"]: norm(p["squad"]) for p in colab}
    head_de = {p["nome_norm"]: p["head_norm"] for p in colab}
    lider_de = {p["nome_norm"]: p["lider_norm"] for p in colab}
    squads_vis, pessoas_vis = None, None
    if head_filter is None:
        pass
    elif head_filter == "__denise__":
        squads_vis = set(DENISE_SQUADS)
        heads = {nn for nn, hd in head_de.items() if hd == DENISE_NORM}
        heads |= {nn for nn, sq in sq_de.items() if sq in DENISE_SQUADS and head_de.get(nn) == nn}
        pessoas_vis = {nn for nn, hd in head_de.items() if hd in heads or hd == DENISE_NORM or nn == DENISE_NORM}
    elif head_filter == "__none__":
        squads_vis = set()
    elif head_filter.startswith("__lider__:"):
        lid = norm(head_filter.split(":", 1)[1])
        squads_vis = {sq_de[lid]} if sq_de.get(lid) else set()
        pessoas_vis = {nn for nn, l in lider_de.items() if l == lid or nn == lid}
    elif head_filter.startswith("__squad__:"):
        squads_vis = {norm(head_filter.split(":", 1)[1])}
    else:
        hd = norm(head_filter)
        squads_vis = {sq for nn, sq in sq_de.items() if head_de.get(nn) == hd and sq}
        pessoas_vis = {nn for nn, h in head_de.items() if h == hd} | {hd}

    def visivel(squad, nn=None):
        if squads_vis is None: return True
        if norm(squad) not in squads_vis: return False
        if pessoas_vis is not None and nn: return nn in pessoas_vis
        return True
    return visivel, pessoas_vis

# ── ABA DO MÊS ────────────────────────────────────────────────
def calcular_abril(mes=None, ano=None, head_filter=None):
    hoje = hoje_brasil()
    mes, ano = mes or hoje.month, ano or hoje.year
    D = carregar_mes(ano, mes)
    colab, metas = D["colab"], D["metas"]
    du_total, du_pass, du_rest = D["du"]["total"], D["du"]["passados"], D["du"]["restantes"]
    ref = date(ano, mes, 1)
    pessoa  = {p["nome_norm"]: p for p in colab}
    meta_de = {m["nome_norm"]: m for m in metas}
    nome_de = {p["nome_norm"]: p["nome"] for p in colab}
    visivel, pessoas_vis = montar_visibilidade(head_filter, colab)

    # Ganhos válidos por (squad da venda, dono) — M1 (head vende pelo funil; resto pelo Colab)
    deals_sq = defaultdict(list)
    for nn, info in D["por_closer"].items():
        for d in info["deals"]:
            deals_sq[(info["squad_por_deal"][d.get("id")], nn)].append(d)

    def linha_closer(nome, meta, deals, is_head=False, refs=0):
        real, multi = m2.bruto(deals), m2.multiplicador(deals)
        mtd = t1.mtd(real, meta, du_total, du_pass)                       # T1-B
        return {
            "nome": nome, "meta": arred(meta), "is_head": is_head,
            "dias_uteis": du_total, "meta_du": arred(safe_div(meta, du_total)),
            "realizado": arred(real),
            "pct_atingido": arred(m3.atingimento(real, meta)),            # M3-B
            "mtd": arred(mtd["meta_mtd"]), "deficit_mtd": arred(mtd["gap"]),
            "pct_mtd": arred(mtd["pct"]),
            "deficit_meta": arred(meta - real),
            # meta/dia restante: sem regra na CRN ainda (pendência T1)
            "meta_dia_100": arred(safe_div(meta - real, du_rest)) if du_rest else 0,
            "realizado_multi": arred(multi),
            "pct_atingido_multi": arred(m3.atingimento(multi, meta)),     # M3-M
            "deficit_meta_multi": arred(meta - multi),
            "meta_dia_multi": arred(safe_div(meta - multi, du_rest)) if du_rest else 0,
            "qtd_ganhos": len(deals),
            "ticket_medio": arred(m2.ticket_medio(deals)),
            "referidos": refs,
            "_deals": deals, "_meta": meta,
        }

    squads = {}
    def get_squad(sq):
        return squads.setdefault(sq, {"nome": sq, "closers_ind": [], "sdrs_ind": []})

    # Closers/IS com meta (R2)
    usados = set()
    for m in metas:
        if m["papel"] != "closer": continue
        p = pessoa.get(m["nome_norm"])
        if not p or not p["squad"] or not visivel(p["squad"]): continue
        nn, sq = m["nome_norm"], p["squad"]
        usados.add((sq, nn))
        get_squad(sq)["closers_ind"].append(
            linha_closer(m["nome"], m["meta_financeira"], deals_sq.get((sq, nn), []), refs=D["referidos"].get(nn, 0)))
    # Todo dono na Colab que vendeu e não tem meta naquele squad (M1)
    for (sq, nn), deals in deals_sq.items():
        if (sq, nn) in usados or not visivel(sq, nn): continue
        p = pessoa.get(nn)
        lider = p is not None and h1.nivel(p, colab) != "membro"
        get_squad(sq)["closers_ind"].append(
            linha_closer(nome_de.get(nn, nn), 0, deals, is_head=lider, refs=D["referidos"].get(nn, 0)))

    # SDRs (R2: meta de reunião > 0)
    def linha_sdr(nome, sq, meta_reu, meta_fin, uid, nn, lider_nome, is_lider=False):
        validadas = D["reunioes"].get(uid, []) if uid is not None else []
        deals_sdr = D["por_sdr"].get(nn, [])
        bruto, multi = m5.bruto(deals_sdr), m5.multiplicador(deals_sdr)
        pct_reu = m6.pct_reunioes(len(validadas), meta_reu)
        pct_fin = m5.pct_financeiro(multi, meta_fin)                       # M5-M
        dev = t1.mtd(0, meta_reu, du_total, du_pass)["meta_mtd"]          # meta MTD (mesma conta da T1/T2)
        return {
            "nome": nome, "subarea": sq, "lider": lider_nome, "is_lider": is_lider,
            "meta_reuniao": meta_reu, "meta_diaria": arred(safe_div(meta_reu, du_total)),
            "validadas": len(validadas), "deveria_estar": arred(dev),
            "faltam": arred(dev - len(validadas)), "pct_reu": arred(pct_reu),
            "meta_ganho": arred(meta_fin), "qtd_ganhos": len(deals_sdr),
            "valor_ganho": arred(bruto), "valor_ganho_multi": arred(multi),
            "pct_ganhos": arred(pct_fin),
            "ticket_medio": arred(m2.ticket_medio(deals_sdr)),
            "pct_final": arred(m7.pct_final(pct_reu, pct_fin, ref)) if not is_lider else 0.0,   # M7-M
            "reunioes_detalhes": [{
                "titulo": D["titulos"].get(a.get("deal_id"), "—"), "deal_id": a.get("deal_id"),
                "data_reuniao": str(atividade_dia(a) or ""), "subject": a.get("subject", ""),
                "sdr": nome, "squad": sq,
            } for a in validadas],
        }

    for m in metas:
        if m["papel"] != "sdr": continue
        nn = m["nome_norm"]; p = pessoa.get(nn)
        if not p or not p["squad"] or not visivel(p["squad"], nn): continue
        lid = p["lider_norm"]
        get_squad(p["squad"])["sdrs_ind"].append(linha_sdr(
            m["nome"], p["squad"], m["meta_reunioes"], m["meta_financeira"],
            D["nn_para_uid"].get(nn), nn, nome_de.get(lid, lid)))

    # Líderes e heads sem linha em Metas que fizeram reunião ou têm ganho como qualificador (R2)
    for p in colab:
        nn = p["nome_norm"]
        if nn in meta_de and meta_de[nn]["papel"] is not None: continue
        if h1.nivel(p, colab) == "membro" or not p["squad"]: continue
        if norm(p["squad"]) in SQUADS_SEM_SDR or not visivel(p["squad"], nn): continue
        linha = linha_sdr(p["nome"], p["squad"], 0, 0, D["nn_para_uid"].get(nn), nn, p["nome"], is_lider=True)
        if linha["validadas"] == 0 and linha["qtd_ganhos"] == 0: continue
        get_squad(p["squad"])["sdrs_ind"].append(linha)

    def total_closers(ind, sq):
        if not ind: return None
        deals = [d for c in ind for d in c["_deals"]]
        meta = sum(c["_meta"] for c in ind)
        if norm(sq) == SQUAD_ASCENSAO:
            meta = as3.meta_time(metas)                                    # AS3: linha da gestora
        t = linha_closer("TOTAL", meta, deals)
        t["referidos"] = sum(c.get("referidos", 0) for c in ind)
        return t

    def total_sdrs(ind):
        if not ind: return None
        t_reu = sum(s["meta_reuniao"] for s in ind); t_val = sum(s["validadas"] for s in ind)
        t_dev = sum(s["deveria_estar"] for s in ind); t_mg = sum(s["meta_ganho"] for s in ind)
        t_bruto = sum(s["valor_ganho"] for s in ind); t_multi = sum(s["valor_ganho_multi"] for s in ind)
        t_qtd = sum(s["qtd_ganhos"] for s in ind)
        pct_r, pct_g = m6.pct_reunioes(t_val, t_reu), m5.pct_financeiro(t_multi, t_mg)
        return {
            "nome": "TOTAL", "subarea": "",
            "meta_reuniao": t_reu, "meta_diaria": arred(safe_div(t_reu, du_total)),
            "validadas": t_val, "deveria_estar": arred(t_dev), "faltam": arred(t_dev - t_val),
            "pct_reu": arred(pct_r), "meta_ganho": arred(t_mg),
            "qtd_ganhos": t_qtd, "valor_ganho": arred(t_bruto), "valor_ganho_multi": arred(t_multi),
            "pct_ganhos": arred(pct_g),
            "ticket_medio": arred(safe_div(t_bruto, t_qtd)) if t_qtd else 0,
            "pct_final": arred(m7.pct_final(pct_r, pct_g, ref)),           # A1-M Sniper: 70/30 sobre os totais
        }

    # Dedup de SDR + filtro final de pessoas (comportamento anterior)
    for sq in squads.values():
        vistos, saida = set(), []
        for s in sq["sdrs_ind"]:
            k = norm(s["nome"])
            if k in vistos or (pessoas_vis is not None and k not in pessoas_vis): continue
            vistos.add(k); saida.append(s)
        sq["sdrs_ind"] = saida

    squads_final = {k: v for k, v in squads.items() if not eh_lic(k)}      # Licenciados fora (recorte)

    squads_result, squads_out = [], []
    for sub, sq in squads_final.items():
        tc, ts = total_closers(sq["closers_ind"], sub), total_sdrs(sq["sdrs_ind"])
        squads_out.append({"nome": sub, "closers": sq["closers_ind"], "closer_total": tc,
                           "sdrs": sq["sdrs_ind"], "sdr_total": ts})
        if norm(sub) in EXCLUIR_CARDS: continue
        so_sdr = a1.squad_so_sdr(sub)
        ating_closer = a1.resultado_financeiro([tc["realizado_multi"]], [tc["_meta"]]) if tc else None   # A1-M
        ating_sdr = ts["pct_final"] if ts else None
        resultado = ating_sdr if so_sdr else ating_closer
        squads_result.append({
            "nome": sub,
            "ating_closer": arred(ating_closer), "ating_sdr": arred(ating_sdr) if ating_sdr is not None else None,
            "resultado": arred(resultado),
            "tem_sdr": so_sdr and ts is not None, "tem_closer": not so_sdr,
            "closer_meta": arred(tc["_meta"]) if tc else 0, "closer_mtd": tc["mtd"] if tc else 0,
            "closer_pct_mtd": tc["pct_mtd"] if tc else 0, "closer_bruto": tc["realizado"] if tc else 0,
            "closer_multi": tc["realizado_multi"] if tc else 0, "closer_vol": tc["qtd_ganhos"] if tc else 0,
            "closer_refs": tc["referidos"] if tc else 0,
            "sdr_meta_reu": arred(ts["meta_reuniao"]) if ts else 0, "sdr_meta_fin": ts["meta_ganho"] if ts else 0,
            "sdr_bruto": ts["valor_ganho"] if ts else 0, "sdr_multi": ts["valor_ganho_multi"] if ts else 0,
            "sdr_reunioes": ts["validadas"] if ts else 0,
        })

    def soma(lst, k): return sum(r.get(k, 0) or 0 for r in lst)

    # Consolidado da Denise — PROVISÓRIO (fora da CRN até a FT1); média como era
    den = [r for r in squads_result if norm(r["nome"]) in DENISE_SQUADS]
    if den:
        d_closer = arred(safe_div(soma(den, "ating_closer"), len(den)))
        squads_result.append({
            "nome": "Denise Mussolin", "ating_closer": d_closer, "ating_sdr": None, "resultado": d_closer,
            "tem_sdr": False, "is_consolidated": True, "provisorio": True,
            "closer_meta": arred(soma(den, "closer_meta")), "closer_mtd": arred(soma(den, "closer_mtd")),
            "closer_pct_mtd": arred(safe_div(soma(den, "closer_bruto"), soma(den, "closer_mtd")) * 100) if soma(den, "closer_mtd") else 0,
            "closer_bruto": arred(soma(den, "closer_bruto")), "closer_multi": arred(soma(den, "closer_multi")),
            "closer_vol": soma(den, "closer_vol"), "closer_refs": soma(den, "closer_refs"),
            "sdr_meta_reu": soma(den, "sdr_meta_reu"), "sdr_meta_fin": arred(soma(den, "sdr_meta_fin")),
            "sdr_bruto": arred(soma(den, "sdr_bruto")), "sdr_multi": arred(soma(den, "sdr_multi")),
            "sdr_reunioes": soma(den, "sdr_reunioes"),
        })

    # Total Geral — A1-M: Σ realizado c/ mult ÷ Σ metas de Closers/IS (J2/AS3 por squad); sem 70/30 do Sniper
    geral = [r for r in squads_result if norm(r["nome"]) not in EXCLUIR_GERAL and not r.get("is_consolidated")]
    if geral:
        tg = a1.resultado_financeiro([r["closer_multi"] for r in geral], [r["closer_meta"] for r in geral])
        squads_result.append({
            "nome": "Total Geral", "ating_closer": arred(tg), "ating_sdr": None, "resultado": arred(tg),
            "tem_sdr": False, "is_total_geral": True,
            "closer_meta": arred(soma(geral, "closer_meta")), "closer_mtd": arred(soma(geral, "closer_mtd")),
            "closer_pct_mtd": arred(safe_div(soma(geral, "closer_bruto"), soma(geral, "closer_mtd")) * 100) if soma(geral, "closer_mtd") else 0,
            "closer_bruto": arred(soma(geral, "closer_bruto")), "closer_multi": arred(soma(geral, "closer_multi")),
            "closer_vol": soma(geral, "closer_vol"), "closer_refs": soma(geral, "closer_refs"),
            "sdr_meta_reu": soma(geral, "sdr_meta_reu"), "sdr_meta_fin": arred(soma(geral, "sdr_meta_fin")),
            "sdr_bruto": arred(soma(geral, "sdr_bruto")), "sdr_multi": arred(soma(geral, "sdr_multi")),
            "sdr_reunioes": soma(geral, "sdr_reunioes"),
        })

    all_c = [c for sq in squads_final.values() for c in sq["closers_ind"]]
    all_s = [s for sq in squads_final.values() for s in sq["sdrs_ind"]]
    return {
        "periodo": {"mes": mes, "ano": ano, "du_total": du_total, "du_passados": du_pass,
                    "du_restantes": du_rest, "atualizado_em": agora_str()},
        "squads": squads_out,
        "resultados": squads_result,
        "total_geral": {"closer": total_closers(all_c, ""), "sdr": total_sdrs(all_s)},
        "carimbo": carimbo("Eixo: data de ganho (vendas) e data da reunião (reuniões) · Resultado: c/ multiplicador; "
                           "MTD bruto · Recorte: com meta + líderes; realizado do squad = todo dono na Colab · "
                           "Squads: todos exc. Licenciados; Total Geral exc. Zenite e Int. Comercial · "
                           "Consolidado Denise: provisório"),
    }

# ── ROTAS BÁSICAS ─────────────────────────────────────────────
@app.route("/")
def index():
    return redirect("/login" if "nome" not in session else "/abril")

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        nome = buscar_usuario(request.form.get("usuario", "").strip(), request.form.get("senha", "").strip())
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
    if "nome" not in session: return redirect("/login")
    nome_s = session["nome"]
    return render_template("abril.html", nome=nome_s, is_master=is_master(nome_s), is_denise=norm(nome_s) == DENISE_NORM)

@app.route("/api/abril")
def api_abril():
    if "nome" not in session: return jsonify({"erro": "Não autenticado"}), 401
    try:
        mes, ano = mes_ano_da_req()
        hf = head_filter_da_sessao(session.get("nome", ""), fontes.carregar_colab(ano, mes))
        return jsonify(limpar_nans(calcular_abril(mes=mes, ano=ano, head_filter=hf)))
    except Exception as e:
        return erro_json(e)

@app.route("/api/debug/metas")
def debug_metas():
    if "nome" not in session: return jsonify({"erro": "Não autenticado"}), 401
    linhas = fontes.ler_csv("URL_METAS")
    hoje = hoje_brasil()
    return jsonify({"colunas": list(linhas[0].keys()) if linhas else [], "primeiras_5_linhas": linhas[:5],
                    "mes_ano": f"{hoje.month}/{hoje.year}"})

@app.route("/api/debug/colab")
def debug_colab():
    if "nome" not in session: return jsonify({"erro": "Não autenticado"}), 401
    linhas = fontes.ler_csv("URL_COLAB")
    return jsonify({"colunas": list(linhas[0].keys()) if linhas else [], "primeiras_5_linhas": linhas[:5]})

# ── EXPORTAR GANHOS (CSV) ─────────────────────────────────────
@app.route("/api/exportar-ganhos")
def exportar_ganhos():
    if "nome" not in session: return jsonify({"erro": "Não autenticado"}), 401
    try:
        mes, ano = mes_ano_da_req()
        D = carregar_mes(ano, mes)
        squad_colab = {p["nome_norm"]: p["squad"] for p in D["colab"]}
        squad_venda = {d_id: sq for info in D["por_closer"].values() for d_id, sq in info["squad_por_deal"].items()}
        situacao = [("Válido", D["sep"]["validos"]), ("Cortesia (R4)", D["sep"]["cortesias"]),
                    ("Outra moeda (R3)", D["sep"]["outra_moeda"]), ("Venda automática (G1)", D["sep"]["automaticos"])]
        out = io.StringIO()
        w = csv.writer(out)
        w.writerow(["ID", "Título", "Proprietário", "Squad", "Funil", "Data Criação", "Data Ganho",
                    "Moeda", "Valor Bruto", "Valor c/ Multiplicador", "Situação"])
        for rotulo, deals in situacao:
            for d in deals:
                dono = m1.nome_dono(d, D["usuarios"])
                if lc1.eh_licenciado(d, D["funis"]):
                    sq = LICENCIADOS
                else:
                    sq = squad_venda.get(d.get("id")) or squad_colab.get(norm(dono), "")
                w.writerow([d.get("id"), d.get("title", ""), dono, sq, D["funis"].get(d.get("pipeline_id"), ""),
                            data_brasil(d.get("add_time")) or "", data_brasil(d.get("won_time")) or "",
                            d.get("currency", ""),
                            f"R$ {float(d.get('value') or 0):,.0f}".replace(",", "."),
                            f"R$ {float(cf(d, campo('multiplicador')) or 0):,.0f}".replace(",", "."),
                            rotulo])
        return Response("﻿" + out.getvalue(), mimetype="text/csv",
                        headers={"Content-Disposition": f"attachment; filename=ganhos_{mes:02d}_{ano}.csv"})
    except Exception as e:
        return erro_json(e)

# ── FORECAST DIÁRIO ───────────────────────────────────────────
def _squad_do_deal(d, nome_squad, funis, usuarios):
    if lc1.eh_licenciado(d, funis): return LICENCIADOS                     # LC1: pelo funil
    return nome_squad.get(norm(m1.nome_dono(d, usuarios)), "")

def calcular_forecast(head_filter=None):
    hoje = hoje_brasil()
    colab = fontes.carregar_colab(hoje.year, hoje.month)                    # F1: Colab do mês atual
    usuarios = pipedrive.usuarios(); funis = pipedrive.funis()
    nome_squad = {p["nome_norm"]: p["squad"] for p in colab}
    por_nome = {p["nome_norm"]: p for p in colab}
    visivel, _ = montar_visibilidade(head_filter, colab)
    if head_filter == "__denise__":
        visivel_sq = lambda sq: norm(sq) in DENISE_FC
    else:
        visivel_sq = lambda sq: visivel(sq)

    deals_fc = memo(("forecast",), lambda: pipedrive.deals(filter_id=filtro("forecast")))
    ini_m, fim_m = limites_mes(hoje.year, hoje.month)
    ganhos = memo(("ganhos", ini_m), lambda: pipedrive.ganhos_do_periodo(ini_m, fim_m))
    validos = m1.separar_ganhos(ganhos, ini=ini_m, fim=fim_m, usuarios=usuarios)["validos"]

    # agrupa: squad → dia → dono → deals do forecast; realizado: squad → dia → dono → valor (M2-B)
    grupo = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    realizado = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    for d in deals_fc:
        sq = _squad_do_deal(d, nome_squad, funis, usuarios)
        if not sq or norm(sq) in FORA_FORECAST or not visivel_sq(sq): continue
        dia = data_brasil(d.get("won_time")) if d.get("status") == "won" else data_local(d.get("expected_close_date"))
        if not dia: continue
        grupo[sq][dia][m1.nome_dono(d, usuarios)].append(d)
    for d in validos:
        dono = m1.nome_dono(d, usuarios)
        if lc1.eh_licenciado(d, funis):
            sq = LICENCIADOS
        else:
            p = por_nome.get(norm(dono))
            if not p: continue
            sq = m1.squad_da_venda(d, p, colab, funis)
        if norm(sq) in FORA_FORECAST or not visivel_sq(sq): continue
        realizado[sq][data_brasil(d.get("won_time"))][dono] += m2.bruto([d])

    def bloco(deals, dia, real):
        fx = f1.faixas(deals, dia, dia)
        perda = f3.perda(deals, dia, dia)
        conv = f4.conversao(real, fx["em_aberto"], perda)
        return {"p20": arred(fx[20]), "p50": arred(fx[50]), "p70": arred(fx[70]),
                "media": arred(f2.media_ponderada(fx)), "em_aberto": arred(fx["em_aberto"]),
                "realizado": arred(real), "perda": arred(perda),
                "total_previsto": arred(fx["em_aberto"] + real + perda),
                "atingimento": arred(conv) if conv is not None else None,
                "pct_atingimento": arred(conv) if conv is not None else None}

    result = {}
    for sq in sorted(set(grupo) | set(realizado)):
        rows = []
        for dia in sorted(set(grupo[sq]) | set(realizado[sq])):
            donos_dia = set(grupo[sq][dia]) | set(realizado[sq][dia])
            todos = [d for lst in grupo[sq][dia].values() for d in lst]
            linha = bloco(todos, dia, sum(realizado[sq][dia].values()))
            closers = []
            for dono in donos_dia:
                c = bloco(grupo[sq][dia].get(dono, []), dia, realizado[sq][dia].get(dono, 0.0))
                c["nome"] = dono
                c["deals"] = [{"id": d.get("id"), "titulo": d.get("title", ""), "valor": arred(d.get("value")),
                               "probabilidade": d.get("probability"), "status": d.get("status")}
                              for d in grupo[sq][dia].get(dono, [])]
                closers.append(c)
            closers.sort(key=lambda x: -(x["realizado"] + x["media"]))
            linha["dia"] = dia.isoformat(); linha["closers"] = closers
            rows.append(linha)
        t = {k: arred(sum(r[k] for r in rows)) for k in ["p20", "p50", "p70", "media", "em_aberto", "realizado", "perda", "total_previsto"]}
        conv = f4.conversao(t["realizado"], t["em_aberto"], t["perda"])
        t["atingimento"] = t["pct_atingimento"] = arred(conv) if conv is not None else None
        result[sq] = {"rows": rows, "total": t}

    # Resumos Time Denise e Geral — PROVISÓRIOS (FT1)
    feriados = fontes.carregar_feriados()
    prox = hoje + timedelta(days=1)
    while prox.weekday() >= 5 or prox in feriados:
        prox += timedelta(days=1)
    def resumo(squads_alvo, dia=None):
        r = defaultdict(float)
        for sq, dados in result.items():
            if norm(sq) not in squads_alvo: continue
            for row in dados["rows"]:
                if dia is None or row["dia"] == dia:
                    for k in ["p20", "p50", "p70", "media", "em_aberto", "realizado", "perda"]:
                        r[k] += row[k]
        return {k: arred(v) for k, v in r.items()}
    h, o, p = hoje.isoformat(), (hoje - timedelta(days=1)).isoformat(), prox.isoformat()
    resumo_fc = {"provisorio": True,
                 "time_denise": resumo(DENISE_FC, h), "geral": resumo(GERAL_FC, h),
                 "time_denise_ontem": resumo(DENISE_FC, o), "geral_ontem": resumo(GERAL_FC, o),
                 "time_denise_prox": resumo(DENISE_FC, p), "geral_prox": resumo(GERAL_FC, p),
                 "time_denise_mes": resumo(DENISE_FC), "geral_mes": resumo(GERAL_FC),
                 "hoje": h, "ontem": o, "prox": p}
    return {"squads": result, "resumo": resumo_fc, "hoje": h, "atualizado_em": agora_str(),
            "carimbo": carimbo("Eixo: data prevista (aberto e perda) e data de ganho (realizado) · Valor: bruto · "
                               "Squad: Colab do mês atual; Licenciados pelo funil · Fonte do pipeline: filtro 1490240 · "
                               "Sniper fora (100% SDR)")}

@app.route("/api/forecast")
def api_forecast():
    if "nome" not in session: return jsonify({"erro": "Não autenticado"}), 401
    try:
        hoje = hoje_brasil()
        hf = head_filter_da_sessao(session.get("nome", ""), fontes.carregar_colab(hoje.year, hoje.month))
        return jsonify(limpar_nans(calcular_forecast(head_filter=hf)))
    except Exception as e:
        return erro_json(e)

@app.route("/api/forecast/periodo")
def api_forecast_periodo():
    if "nome" not in session: return jsonify({"erro": "Não autenticado"}), 401
    try:
        if not request.args.get("ini") or not request.args.get("fim"):
            return jsonify({"erro": "Parâmetros ini e fim obrigatórios"}), 400
        d_ini, d_fim = date.fromisoformat(request.args["ini"]), date.fromisoformat(request.args["fim"])
        hoje = hoje_brasil()
        colab = fontes.carregar_colab(hoje.year, hoje.month)
        usuarios, funis = pipedrive.usuarios(), pipedrive.funis()
        nome_sess = session.get("nome", "")
        todos = is_superuser(nome_sess) or norm(nome_sess) == DENISE_NORM
        hd = norm(nome_sess)
        squads_vis = None if todos else {norm(p["squad"]) for p in colab if p["head_norm"] == hd and p["squad"]}
        squad_visivel = lambda sq: squads_vis is None or norm(sq) in squads_vis

        # Realizado do período (M2-B): só o período pedido
        ganhos = pipedrive.ganhos_do_periodo(d_ini, d_fim)
        validos = m1.separar_ganhos(ganhos, ini=d_ini, fim=d_fim, usuarios=usuarios)["validos"]
        por_nome = {p["nome_norm"]: p for p in colab}
        realizado = defaultdict(float)
        for d in validos:
            if lc1.eh_licenciado(d, funis):
                sq = LICENCIADOS
            else:
                p = por_nome.get(norm(m1.nome_dono(d, usuarios)))
                if not p: continue
                sq = m1.squad_da_venda(d, p, colab, funis)
            if squad_visivel(sq): realizado[sq] += m2.bruto([d])

        fc = calcular_forecast(head_filter=None if todos else nome_sess)["squads"]
        acc = {k: defaultdict(float) for k in ["em_aberto", "perda", "media", "p20", "p50", "p70"]}
        for sq, dados in fc.items():
            if not squad_visivel(sq): continue
            for row in dados["rows"]:
                if request.args["ini"] <= row["dia"] <= request.args["fim"]:
                    for k in acc: acc[k][sq] += row.get(k, 0)

        excluir = {"zenite", "licenciados"}
        ordem = ["Sniper", "Elite", "Olympus", "LATAM", "Vanguard"]
        todos_sq = set(realizado) | set(acc["em_aberto"]) | set(acc["perda"])
        def mk(sq, nome=None, lista=None):
            lst = lista or [sq]
            r = sum(realizado.get(s, 0) for s in lst); e = sum(acc["em_aberto"].get(s, 0) for s in lst)
            return {"squad": nome or sq, "realizado": arred(r), "em_aberto": arred(e),
                    "media": arred(sum(acc["media"].get(s, 0) for s in lst)),
                    "perda": arred(sum(acc["perda"].get(s, 0) for s in lst)),
                    "p20": arred(sum(acc["p20"].get(s, 0) for s in lst)),
                    "p50": arred(sum(acc["p50"].get(s, 0) for s in lst)),
                    "p70": arred(sum(acc["p70"].get(s, 0) for s in lst)),
                    "total_prev": arred(r + e)}
        squads_out = [mk(sq) for sq in ordem if sq in todos_sq and norm(sq) not in excluir]
        squads_out += [mk(sq) for sq in sorted(todos_sq) if sq not in ordem and norm(sq) not in excluir]
        return jsonify(limpar_nans({
            "squads": squads_out,
            "denise": mk(None, "Time Denise · Provisório", [s for s in todos_sq if norm(s) in DENISE_FC]),
            "geral": mk(None, "Geral · Provisório", list(todos_sq)),
            "ini": request.args["ini"], "fim": request.args["fim"], "atualizado_em": agora_str(),
        }))
    except Exception as e:
        return erro_json(e)

# ── OVERVIEW (JACARÉ = MTD em gráfico) ────────────────────────
def calcular_overview(mes=None, ano=None, head_filter=None, is_denise=False):
    hoje = hoje_brasil()
    mes, ano = mes or hoje.month, ano or hoje.year
    abril_data = calcular_abril(mes=mes, ano=ano, head_filter=None)
    D = carregar_mes(ano, mes)
    du_total = D["du"]["total"]
    uteis = fontes.dias_uteis_calendario(ano, mes, D["feriados"])
    ini, fim = D["ini"], D["fim"]
    todos_dias = [ini + timedelta(days=i) for i in range((fim - ini).days + 1)]
    du_ate = {d: sum(1 for u in uteis if u <= d) for d in todos_dias}

    # Meta por squad = total de closers da aba do mês (M3 / AS3 / J2)
    meta_sq = {sq["nome"]: (sq["closer_total"]["_meta"] if sq.get("closer_total") else 0.0)
               for sq in abril_data["squads"]}

    ganhos_dia = defaultdict(lambda: defaultdict(float))
    for nn, info in D["por_closer"].items():
        for d in info["deals"]:
            ganhos_dia[info["squad_por_deal"][d.get("id")]][data_brasil(d.get("won_time"))] += m2.bruto([d])

    # Sniper: reuniões validadas por dia (M6) e meta de reuniões
    sniper_nn = {p["nome_norm"] for p in D["colab"] if a1.squad_so_sdr(p["squad"])}
    uid_sniper = {D["nn_para_uid"][nn] for nn in sniper_nn if nn in D["nn_para_uid"]}
    reu_dia = defaultdict(int)
    for uid, lst in D["reunioes"].items():
        if uid in uid_sniper:
            for a in lst: reu_dia[atividade_dia(a)] += 1
    meta_reu_sniper = sum(m["meta_reunioes"] for m in D["metas"] if m["nome_norm"] in sniper_nn and m["papel"] == "sdr")

    def serie(meta, por_dia, meta_reu=None, reu=None):
        dias, acum, acum_r = [], 0.0, 0
        for dia in todos_dias:
            acum += por_dia.get(dia, 0.0)
            if reu is not None: acum_r += reu.get(dia, 0)
            dias.append({"dia": dia.isoformat(),
                         "meta_mtd": arred(t1.mtd(0, meta, du_total, du_ate[dia])["meta_mtd"]),
                         "real_mtd": arred(acum),
                         "meta_reu_mtd": arred(t1.mtd(0, meta_reu, du_total, du_ate[dia])["meta_mtd"]) if reu is not None else None,
                         "reu_mtd": acum_r if reu is not None else None})
        return dias

    result = {}
    for sq in sorted(set(meta_sq) | set(ganhos_dia)):
        if eh_lic(sq) or norm(sq) == "licenciados": continue
        sdr = a1.squad_so_sdr(sq)
        result[sq] = {"dias": serie(meta_sq.get(sq, 0.0), ganhos_dia[sq],
                                    meta_reu_sniper if sdr else None, reu_dia if sdr else None),
                      "meta_total": arred(meta_sq.get(sq, 0.0)), "is_sdr_squad": sdr,
                      "meta_reu_total": arred(meta_reu_sniper) if sdr else None}

    if is_denise:   # consolidado PROVISÓRIO (FT1)
        den = [sq for sq in result if sq in ("Sniper", "Elite", "Olympus")]
        if den:
            g = defaultdict(float)
            for sq in den:
                for dia, v in ganhos_dia[sq].items(): g[dia] += v
            meta_d = sum(meta_sq.get(sq, 0) for sq in den)
            result["__DENISE__"] = {"dias": serie(meta_d, g), "meta_total": arred(meta_d), "provisorio": True}

    # Total: Σ metas de Closers/IS (J2/AS3) e Σ ganhos, sem Zenite, Int. Comercial e Licenciados
    tot_sq = [sq for sq in result if not sq.startswith("__") and norm(sq) not in EXCLUIR_GERAL]
    g_tot = defaultdict(float)
    for sq in tot_sq:
        for dia, v in ganhos_dia[sq].items(): g_tot[dia] += v
    meta_tot = sum(meta_sq.get(sq, 0) for sq in tot_sq)
    result["__TOTAL__"] = {"dias": serie(meta_tot, g_tot), "meta_total": arred(meta_tot)}

    return {"squads": result, "is_denise": is_denise, "is_master": head_filter is None,
            "mes": mes, "ano": ano, "atualizado_em": agora_str(),
            "carimbo": carimbo("Eixo: data de ganho; Sniper por data da reunião · Valor: bruto · "
                               "Meta: J2 (Closers/IS; Ascensão pela gestora) · Total exc. Zenite, Int. Comercial e Licenciados")}

@app.route("/api/overview")
def api_overview():
    if "nome" not in session: return jsonify({"erro": "Não autenticado"}), 401
    try:
        mes, ano = mes_ano_da_req()
        nome_sess = session.get("nome", "")
        hf = head_filter_da_sessao(nome_sess, fontes.carregar_colab(ano, mes))
        return jsonify(limpar_nans(calcular_overview(mes=mes, ano=ano, head_filter=hf,
                                                     is_denise=norm(nome_sess) == DENISE_NORM)))
    except Exception as e:
        return erro_json(e)

# ── RANKING ───────────────────────────────────────────────────
def calcular_ranking(mes=None, ano=None):
    hoje = hoje_brasil()
    mes, ano = mes or hoje.month, ano or hoje.year
    D = carregar_mes(ano, mes)
    ref = date(ano, mes, 1)
    pessoa = {p["nome_norm"]: p for p in D["colab"]}
    closers, sdrs = [], []
    for m in D["metas"]:
        p = pessoa.get(m["nome_norm"])
        if not p or not p["squad"]: continue
        nn = m["nome_norm"]
        if m["papel"] == "closer":
            deals = D["por_closer"].get(nn, {}).get("deals", [])
            real = m2.bruto(deals)                                                  # M2-B
            closers.append({"nome": m["nome"], "time": p["squad"], "meta": arred(m["meta_financeira"]),
                            "realizado": arred(real),
                            "pct_atingido": arred(m3.atingimento(real, m["meta_financeira"]))})   # M3-B
        elif m["papel"] == "sdr":
            uid = D["nn_para_uid"].get(nn)
            validadas = len(D["reunioes"].get(uid, [])) if uid is not None else 0
            bruto = m5.bruto(D["por_sdr"].get(nn, []))                              # M5-B
            pct_reu = m6.pct_reunioes(validadas, m["meta_reunioes"])
            pct_fin = m5.pct_financeiro(bruto, m["meta_financeira"])
            sdrs.append({"nome": m["nome"], "time": p["squad"], "meta_reu": arred(m["meta_reunioes"]),
                         "realizado_reu": validadas, "meta_fin": arred(m["meta_financeira"]),
                         "realizado_fin": arred(bruto), "pct_reu": arred(pct_reu), "pct_fin": arred(pct_fin),
                         "pct_final": arred(m7.pct_final(pct_reu, pct_fin, ref))})             # M7-B
    times = sorted({c["time"] for c in closers} | {s["time"] for s in sdrs})
    return {"closers": closers, "sdrs": sdrs, "times": times, "mes": mes, "ano": ano,
            "atualizado_em": agora_str(),
            "carimbo": carimbo("Eixo: data de ganho e data da reunião · Valor: bruto (M3-B, M5-B, M7-B) · Recorte: com meta")}

@app.route("/api/ranking")
def api_ranking():
    if "nome" not in session: return jsonify({"erro": "Não autenticado"}), 401
    try:
        mes, ano = mes_ano_da_req()
        return jsonify(limpar_nans(calcular_ranking(mes=mes, ano=ano)))
    except Exception as e:
        return erro_json(e)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
