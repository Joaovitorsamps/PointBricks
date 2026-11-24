import hashlib
import json
import math
import random
import re
import time
import unicodedata
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
from io import BytesIO
from urllib.parse import quote
from zipfile import BadZipFile, ZipFile


def _sessao_spark():
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("Inicie uma sessão Spark antes de usar esta função.")
    return spark


_EXCEL_PADROES = {
    "max_mb_por_arquivo": 50,
    "max_mb_descompactado": 400,
    "max_linhas_total": 350_000,
    "max_linhas_por_aba": 450_000,
    "max_colunas_por_aba": 200,
    "permitir_arquivo_vazio": False,
    "permitir_base_vazia": False,
}


class ExcelQualidadeError(ValueError):
    """Configuração ou qualidade reprovada; não aplicar o lote."""


def _excel_exigir(condicao, mensagem):
    if not condicao:
        raise ExcelQualidadeError(mensagem)


# Nome de coluna: inicia por letra (acentos permitidos); depois letras, números, sublinhado e
# espaços simples internos. Pontos, crases, aspas, hífens e demais símbolos são rejeitados.
# Iniciar por letra também impede colisão com colunas internas (_fonte, _linha, _excel_*, _primeira_*...).
# Nomes ASCII antigos ([A-Za-z][A-Za-z0-9_]*) continuam válidos e inalterados.
_EXCEL_NOME_COLUNA = re.compile(r"[^\W\d_](?:[\w ]*\w)?")


def _excel_nome_coluna_valido(nome):
    return (isinstance(nome, str) and _EXCEL_NOME_COLUNA.fullmatch(nome) is not None
            and "  " not in nome and unicodedata.is_normalized("NFC", nome))


def _excel_col(nome):
    """Referência literal a uma coluna: nunca caminho aninhado (a.b) nem expressão SQL."""
    from pyspark.sql import functions as F

    return F.col("`" + nome.replace("`", "``") + "`")


def _excel_caminho_variant(nome):
    """Caminho VARIANT literal para um campo de primeiro nível ($['Valor Líquido'])."""
    _excel_exigir("'" not in nome and "\\" not in nome, f"Nome de coluna inválido: {nome!r}")
    return f"$['{nome}']"


def _excel_contrato(schema, chaves, obrigatorias, regras):
    _excel_exigir(isinstance(schema, dict) and bool(schema), "Informe um schema não vazio.")
    nomes = list(schema)
    _excel_exigir(all(isinstance(n, str) for n in nomes), "Nomes devem ser strings.")
    _excel_exigir(len({n.lower() for n in nomes}) == len(nomes)
                  and len({n.casefold() for n in nomes}) == len(nomes),
                  "Colunas não podem diferir somente por maiúsculas/minúsculas.")
    for nome, tipo in schema.items():
        _excel_exigir(
            _excel_nome_coluna_valido(nome),
            f"Nome de coluna inválido: {nome!r}. Inicie por letra e use apenas letras "
            "(acentos permitidos, forma Unicode NFC), números, sublinhado e espaços simples "
            "internos; pontos, crases, aspas e outros símbolos não são aceitos.",
        )
        _excel_exigir(
            isinstance(tipo, str)
            and re.fullmatch(r"string|bigint|date|boolean|double|decimal\(\d+,\d+\)", tipo),
            f"Tipo não suportado: {tipo}",
        )
        if tipo.startswith("decimal"):
            p, s = map(int, re.findall(r"\d+", tipo))
            _excel_exigir(0 <= s <= p <= 38 and p >= 1, f"Precisão/escala inválida: {tipo}")
    _excel_exigir(not isinstance(chaves, str), "Chaves devem ser uma lista, não uma string.")
    chaves = list(chaves or [])
    _excel_exigir(len(chaves) == len(set(chaves)), "Chave repetida.")
    _excel_exigir(obrigatorias is None or not isinstance(obrigatorias, str),
                  "Obrigatórias devem ser uma lista.")
    obrigatorias = list(schema) if obrigatorias is None else list(obrigatorias)
    _excel_exigir(set(chaves + obrigatorias) <= set(schema),
                  "Chaves/obrigatórias precisam existir no schema.")
    _excel_exigir(all(schema[c] == "string" for c in chaves),
                  "Chaves devem ter tipo string.")
    regras = dict(regras or {})
    for nome, expr in regras.items():
        _excel_exigir(isinstance(expr, str) and bool(expr.strip()), f"Regra vazia: {nome}")
    return chaves, sorted(set(chaves + obrigatorias)), regras


def _excel_config_leitura(arquivos, schema, chaves, limites, formatos=None, localidade=None):
    _excel_exigir(isinstance(arquivos, (list, tuple)) and bool(arquivos),
                  "Informe uma lista não vazia de arquivos.")
    limites = dict(limites or {})
    _excel_exigir(not (set(limites) - set(_EXCEL_PADROES)),
                  "Há limites de leitura desconhecidos.")
    cfg = {**deepcopy(_EXCEL_PADROES), **limites,
           "colunas": dict(schema), "chaves": list(chaves), "arquivos": deepcopy(arquivos),
           "formatos": _excel_compilar_formatos(formatos, schema, localidade)}
    for campo in ("max_mb_por_arquivo", "max_mb_descompactado", "max_linhas_total",
                  "max_linhas_por_aba", "max_colunas_por_aba"):
        _excel_exigir(type(cfg[campo]) is int and cfg[campo] > 0,
                      f"{campo} deve ser inteiro > 0.")
    for campo in ("permitir_arquivo_vazio", "permitir_base_vazia"):
        _excel_exigir(type(cfg[campo]) is bool, f"{campo} deve ser True/False.")
    fontes = []
    for arquivo in arquivos:
        _excel_exigir(isinstance(arquivo, dict), "Cada arquivo deve ser um dicionário.")
        for campo in ("drive_id", "item_id", "aba"):
            _excel_exigir(isinstance(arquivo.get(campo), str) and arquivo[campo].strip(),
                          f"Arquivo sem {campo} válido.")
        linha = arquivo.get("linha_cabecalho", 1)
        _excel_exigir(type(linha) is int and linha >= 1, "Cabeçalho deve ser inteiro >= 1.")
        fontes.append((arquivo["drive_id"], arquivo["item_id"], arquivo["aba"]))
    _excel_exigir(len(fontes) == len(set(fontes)), "Fonte/aba configurada duas vezes.")
    return cfg


_EXCEL_ESCOPO_GRAPH = "https://graph.microsoft.com/.default"


def conectar_sharepoint(credencial):
    """Autentica com uma credencial síncrona e a devolve para ler_excel_validado."""
    _excel_exigir(callable(getattr(credencial, "get_token", None)),
                  "Forneça uma credencial síncrona do Azure Identity.")
    credencial.get_token(_EXCEL_ESCOPO_GRAPH)
    return credencial


class _ExcelGraph:
    BASE = "https://graph.microsoft.com/v1.0"

    def __init__(self, login):
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        self.login = login
        self.http = requests.Session()
        retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
                      allowed_methods=["GET"], respect_retry_after_header=True,
                      raise_on_status=False)
        self.http.mount("https://", HTTPAdapter(max_retries=retry))

    def _endpoint(self, arquivo):
        drive = quote(arquivo["drive_id"], safe="")
        item = quote(arquivo["item_id"], safe="")
        return f"{self.BASE}/drives/{drive}/items/{item}"

    def _get_graph(self, url, stream=False):
        for tentativa in range(2):
            resposta = self.http.get(
                url, headers={"Authorization": f"Bearer {self.login.get_token(_EXCEL_ESCOPO_GRAPH).token}"},
                allow_redirects=False, stream=stream, timeout=(15, 120),
            )
            if resposta.status_code != 401 or tentativa == 1:
                return resposta
            resposta.close()

    def metadata(self, arquivo):
        import requests

        try:
            with self._get_graph(self._endpoint(arquivo)) as resposta:
                if resposta.status_code != 200:
                    raise RuntimeError(f"Metadados Graph: HTTP {resposta.status_code}.")
                meta = resposta.json()
        except requests.RequestException:
            raise RuntimeError("Falha de rede consultando metadados Graph.") from None
        _excel_exigir("file" in meta, "A origem não é um arquivo.")
        _excel_exigir(bool(meta.get("eTag")), "Arquivo sem eTag.")
        _excel_exigir(meta["name"].lower().endswith(".xlsx"), "Somente .xlsx são aceitos.")
        return meta

    def baixar(self, arquivo, limite_bytes):
        import requests

        resposta = None
        try:
            resposta = self._get_graph(self._endpoint(arquivo) + "/content", stream=True)
            if resposta.status_code in (301, 302, 303, 307, 308):
                url = resposta.headers.get("Location", "")
                resposta.close()
                _excel_exigir(url.startswith("https://"), "Download sem URL HTTPS.")
                resposta = self.http.get(url, stream=True, timeout=(15, 120))
            if resposta.status_code != 200:
                raise RuntimeError(f"Download Excel: HTTP {resposta.status_code}.")
            buffer = BytesIO()
            for parte in resposta.iter_content(chunk_size=1024 * 1024):
                if parte:
                    _excel_exigir(buffer.tell() + len(parte) <= limite_bytes,
                                  "Download excede o limite configurado.")
                    buffer.write(parte)
            return buffer.getvalue()
        except requests.RequestException:
            raise RuntimeError("Falha de rede durante download Excel.") from None
        finally:
            if resposta is not None:
                resposta.close()

    def close(self):
        self.http.close()


def _excel_vazio(valor):
    return valor is None or (isinstance(valor, str) and not valor.strip())


_EXCEL_DIGITOS_SIGNIFICATIVOS = 15  # mesma precisão que o Excel exibe e calcula


def _excel_numero_texto(valor):
    _excel_exigir(math.isfinite(valor), "Número não finito.")
    numero = Decimal(format(valor, f".{_EXCEL_DIGITOS_SIGNIFICATIVOS}g"))
    if numero == 0:
        return "0"
    return format(numero.normalize(), "f")


def _excel_texto(valor):
    if _excel_vazio(valor):
        return None
    if isinstance(valor, str):
        return valor.strip()
    if isinstance(valor, bool):
        return "true" if valor else "false"
    if isinstance(valor, datetime):
        _excel_exigir(valor.time().isoformat() == "00:00:00", "Data contém horário não suportado.")
        return valor.date().isoformat()
    if isinstance(valor, date):
        return valor.isoformat()
    if isinstance(valor, float):
        return _excel_numero_texto(valor)
    return str(valor)


_FmtDecimal = Decimal

_FMT_PADRAO_DATAS = {
    "data": ["%Y-%m-%d", "%d/%m/%Y"],
    "periodo": ["%Y-%m", "%m/%Y", "%Y-%m-%d", "%d/%m/%Y"],
}

# Localidade declarada pelo consumidor: define como datas e números decimais
# aparecem em texto. ISO (AAAA-MM-DD) é aceito em todas, por não ser ambíguo.
# Não se aplica a IDs nem a inteiros.
_FMT_LOCALIDADES = {
    "pt_BR": {
        "separador_decimal": ",",
        "separador_milhar": ".",
        "data": ["%d/%m/%Y", "%Y-%m-%d"],
        "periodo": ["%m/%Y", "%d/%m/%Y", "%Y-%m", "%Y-%m-%d"],
    },
    "en_US": {
        "separador_decimal": ".",
        "separador_milhar": ",",
        "data": ["%m/%d/%Y", "%Y-%m-%d"],
        "periodo": ["%m/%Y", "%m/%d/%Y", "%Y-%m", "%Y-%m-%d"],
    },
}
_FMT_TIPOS_COM_LOCALIDADE = ("data", "periodo", "decimal", "moeda")


def _fmt_validar_localidade(localidade, contexto):
    _excel_exigir(localidade is None or localidade in _FMT_LOCALIDADES,
                  f"{contexto}: localidade deve ser None ou uma de {sorted(_FMT_LOCALIDADES)}.")


def _excel_compilar_formatos(formatos, schema, localidade=None):
    """Valida a configuração de formatos contra o schema e aplica os padrões.

    Precedência: opção explícita da coluna > localidade da coluna >
    localidade geral > padrões atuais (sem localidade nada muda).
    """
    _fmt_validar_localidade(localidade, "localidade")
    if not formatos:
        return {}
    _excel_exigir(isinstance(formatos, dict), "formatos deve ser um dicionário {coluna: regra}.")
    gerais = {"tipo", "localidade"}
    datas = {"formatos_texto", "aceitar_horario"}
    numericos = {"separador_decimal", "separador_milhar", "simbolo"}
    compilados = {}
    for coluna, regra in formatos.items():
        _excel_exigir(coluna in schema, f"{coluna}: formato para coluna fora do schema.")
        _excel_exigir(isinstance(regra, dict) and "tipo" in regra,
                      f"{coluna}: regra de formato deve ser dict com 'tipo'.")
        regra = deepcopy(regra)
        tipo = regra["tipo"]
        destino = schema[coluna]

        _excel_exigir(
            tipo in ("periodo", "data", "moeda", "decimal", "inteiro", "id"),
            f"{coluna}: tipo de interpretação desconhecido.",
        )

        identificadores = {"largura_numerica", "texto_decimal_inteiro"}

        permitidos = gerais | (
            identificadores if tipo == "id"
            else datas if tipo in ("periodo", "data")
            else numericos
        )
        _excel_exigir(not (set(regra) - permitidos),
                      f"{coluna}: opções não suportadas para {tipo}: {sorted(set(regra) - permitidos)}.")

        if "localidade" in regra:
            _excel_exigir(tipo in _FMT_TIPOS_COM_LOCALIDADE,
                          f"{coluna}: localidade se aplica só a {', '.join(_FMT_TIPOS_COM_LOCALIDADE)}.")
            _fmt_validar_localidade(regra["localidade"], coluna)
        else:
            regra["localidade"] = localidade if tipo in _FMT_TIPOS_COM_LOCALIDADE else None
        local = _FMT_LOCALIDADES.get(regra["localidade"])

        if tipo == "id":
            _excel_exigir(
                destino == "string",
                f"{coluna}: ID exige schema string.",
            )

            regra.setdefault("largura_numerica", None)
            regra.setdefault("texto_decimal_inteiro", False)

            largura = regra["largura_numerica"]

            _excel_exigir(
                largura is None
                or (type(largura) is int and 1 <= largura <= 256),
                f"{coluna}: largura_numerica deve ser None ou inteiro entre 1 e 256.",
            )

            _excel_exigir(
                type(regra["texto_decimal_inteiro"]) is bool,
                f"{coluna}: texto_decimal_inteiro deve ser booleano.",
            )

            compilados[coluna] = regra
            continue

        if tipo in ("periodo", "data"):
            _excel_exigir(destino == "date", f"{coluna}: {tipo} exige schema date.")
            regra.setdefault("formatos_texto", list((local or _FMT_PADRAO_DATAS)[tipo]))
            regra.setdefault("aceitar_horario", False)
            padroes = regra["formatos_texto"]
            _excel_exigir(isinstance(padroes, (list, tuple)) and bool(padroes)
                          and all(isinstance(p, str) and ("%Y" in p or "%y" in p) for p in padroes),
                          f"{coluna}: formatos_texto deve ser lista de padrões strptime com ano.")
            regra["formatos_texto"] = list(padroes)
            _excel_exigir(type(regra["aceitar_horario"]) is bool,
                          f"{coluna}: aceitar_horario deve ser booleano.")
            compilados[coluna] = regra
            continue

        # Numéricos: inteiro, decimal, moeda.
        if tipo == "inteiro":
            _excel_exigir(destino == "bigint", f"{coluna}: inteiro exige schema bigint.")
        elif tipo == "moeda":
            _excel_exigir(destino.startswith("decimal"), f"{coluna}: moeda exige schema decimal(p,s).")
        else:
            _excel_exigir(destino.startswith("decimal") or destino == "double",
                          f"{coluna}: decimal exige schema decimal(p,s) ou double.")
        if local and "separador_decimal" not in regra:
            regra["separador_decimal"] = local["separador_decimal"]
            regra.setdefault("separador_milhar", local["separador_milhar"])
        regra.setdefault("separador_decimal", ",")
        regra.setdefault("separador_milhar", "." if regra["separador_decimal"] == "," else None)
        regra.setdefault("simbolo", "R$" if tipo == "moeda" else None)
        _excel_exigir(regra["separador_decimal"] in (",", "."),
                      f"{coluna}: separador_decimal deve ser ',' ou '.'.")
        _excel_exigir(regra["separador_milhar"] in (None, ".", ",", " ")
                      and regra["separador_milhar"] != regra["separador_decimal"],
                      f"{coluna}: separador_milhar inválido ou igual ao decimal.")
        _excel_exigir(regra["simbolo"] is None
                      or (isinstance(regra["simbolo"], str) and regra["simbolo"].strip()),
                      f"{coluna}: simbolo deve ser None ou texto.")
        dec = re.escape(regra["separador_decimal"])
        if regra["separador_milhar"] is None:
            corpo = r"[0-9]+"
        else:
            mil = re.escape(regra["separador_milhar"])
            corpo = rf"(?:[0-9]{{1,3}}(?:{mil}[0-9]{{3}})+|[0-9]+)"
        regra["_padrao"] = re.compile(rf"([+-]?)({corpo})(?:{dec}([0-9]+))?")
        # Com localidade declarada, espaços só são aceitos entre sinal, símbolo
        # e valor; dentro dos dígitos ("12 34") o valor não bate com o formato.
        regra["_estrito"] = local is not None
        compilados[coluna] = regra
    return compilados


def _fmt_id(valor, regra):
    """Padroniza IDs numéricos integrais e preserva IDs textuais."""
    if isinstance(valor, str):
        texto = valor.strip()

        if regra["texto_decimal_inteiro"]:
            achado = re.fullmatch(
                r"([+-]?[0-9]+)[.,]([0-9]+)",
                texto,
            )

            if achado:
                _excel_exigir(
                    set(achado[2]) <= {"0"},
                    "ID textual numérico contém fração",
                )
                texto = achado[1]

    else:
        _excel_exigir(
            isinstance(valor, (int, float, _FmtDecimal))
            and not isinstance(valor, bool),
            "ID deve ser texto ou número inteiro",
        )

        numero = _FmtDecimal(str(valor))

        _excel_exigir(
            numero.is_finite(),
            "ID não pode ser NaN/infinito",
        )

        _excel_exigir(
            numero == numero.to_integral_value(),
            "ID numérico contém fração",
        )

        _excel_exigir(
            abs(numero) < _FmtDecimal("1000000000000000"),
            "ID numérico com mais de 15 dígitos deve estar como texto na origem",
        )

        texto = str(int(numero))

    largura = regra["largura_numerica"]

    # O preenchimento é aplicado somente a códigos compostos por dígitos.
    if largura is not None and re.fullmatch(r"[0-9]+", texto):
        _excel_exigir(
            len(texto) <= largura,
            "ID excede a largura numérica declarada",
        )
        texto = texto.zfill(largura)

    return texto


def _fmt_data(valor, regra, epoch=None):
    """Converte data/período para ISO AAAA-MM-DD; período vira o dia 1 do mês."""
    from openpyxl.utils.datetime import WINDOWS_EPOCH, from_excel as _excel_serial_para_data

    periodo = regra["tipo"] == "periodo"
    if isinstance(valor, datetime):
        _excel_exigir(periodo or regra["aceitar_horario"] or valor.time().isoformat() == "00:00:00",
                      "Data contém horário; declare aceitar_horario=True para descartá-lo.")
        dia = valor.date()
    elif isinstance(valor, date):
        dia = valor
    elif isinstance(valor, (int, float)) and not isinstance(valor, bool):
        # Número de série do Excel (célula com formato Geral): 1 = 1900-01-01; 109574 = 2199-12-31.
        _excel_exigir(math.isfinite(valor) and 1 <= valor <= 109574,
                      "Número fora da faixa de datas do Excel.")
        # Número de série depende da data base da pasta de trabalho (1900 ou 1904).
        convertido = _excel_serial_para_data(valor, epoch=epoch or WINDOWS_EPOCH)
        _excel_exigir(periodo or regra["aceitar_horario"] or float(valor).is_integer(),
                      "Data contém horário; declare aceitar_horario=True para descartá-lo.")
        dia = convertido.date() if isinstance(convertido, datetime) else convertido
    elif isinstance(valor, str):
        texto = valor.strip()
        dia = None
        for padrao in regra["formatos_texto"]:
            try:
                dia = datetime.strptime(texto, padrao).date()
                break
            except ValueError:
                continue
        _excel_exigir(dia is not None, "Data fora dos formatos_texto declarados.")
    else:
        raise ExcelQualidadeError("Valor não interpretável como data.")
    return (dia.replace(day=1) if periodo else dia).isoformat()


def _fmt_numero(valor, regra):
    """Converte número/texto numérico para o texto canônico com ponto decimal."""
    _excel_exigir(not isinstance(valor, bool), "Booleano não é número.")
    if isinstance(valor, float):
        texto = _excel_numero_texto(valor)
    elif isinstance(valor, (int, _FmtDecimal)):
        _excel_exigir(_FmtDecimal(valor).is_finite(), "Número não finito.")
        texto = format(_FmtDecimal(valor), "f")
    elif isinstance(valor, str):
        bruto = valor.replace("\u00a0", " ").strip()
        if regra["simbolo"] and regra["simbolo"] in bruto:
            # O símbolo só pode ficar no início (depois do sinal) ou no fim.
            # No meio do número ("10R$5") ele juntaria dígitos sem aviso.
            simbolo = re.escape(regra["simbolo"])
            inicio = re.fullmatch(rf"([+-]?)\s*{simbolo}(.*)", bruto, re.S)
            fim = re.fullmatch(rf"(.*?)\s*{simbolo}", bruto, re.S)
            if inicio:
                bruto = (inicio[1] + inicio[2]).strip()
            elif fim:
                bruto = fim[1].strip()
            else:
                raise ExcelQualidadeError(
                    "Símbolo monetário fora do início ou do fim do valor.")
        if regra.get("_estrito"):
            sinal_separado = re.fullmatch(r"([+-])\s+(.*)", bruto, re.S)
            if sinal_separado:
                bruto = sinal_separado[1] + sinal_separado[2]
        elif regra["separador_milhar"] != " ":
            bruto = bruto.replace(" ", "")
        achado = regra["_padrao"].fullmatch(bruto)
        _excel_exigir(achado is not None, "Texto numérico fora do formato declarado.")
        sinal, inteiro, fracao = achado[1], achado[2], achado[3] or ""
        if regra["separador_milhar"]:
            inteiro = inteiro.replace(regra["separador_milhar"], "")
        texto = f"{'-' if sinal == '-' else ''}{inteiro}" + (f".{fracao}" if fracao else "")
    else:
        raise ExcelQualidadeError("Valor não interpretável como número.")
    if "." in texto:
        # Zeros finais na fração não mudam o valor e não devem estourar a escala.
        texto = texto.rstrip("0").rstrip(".")
    if regra["tipo"] == "inteiro":
        _excel_exigir("." not in texto, "Inteiro contém fração.")
    return "0" if texto in ("-0", "") else texto


def _excel_normalizar(valor, regra, epoch=None):
    """Aplica o formato declarado da coluna; sem formato, mantém a leitura estrita."""
    if regra is None:
        return _excel_texto(valor)
    if _excel_vazio(valor):
        return None
    if regra["tipo"] == "id":
        return _fmt_id(valor, regra)
    if regra["tipo"] in ("periodo", "data"):
        return _fmt_data(valor, regra, epoch)
    return _fmt_numero(valor, regra)


def _excel_valor_celula(cell, cache, local):
    """Valor efetivo da célula: o literal ou, em fórmulas, o resultado armazenado no arquivo.

    cell vem da visão de fórmulas (data_only=False); cache, da visão de resultados (data_only=True).
    O Excel não é recalculado. Erro Excel e fórmula sem resultado armazenado são rejeitados.
    Fórmula com resultado texto vazio ("") é indistinguível de fórmula sem resultado no openpyxl
    e também é rejeitada. Zero e FALSE são resultados válidos.
    """
    if cell.data_type == "e":
        raise ExcelQualidadeError(f"{local}: fórmula ou erro Excel.")
    if cell.data_type != "f":
        return cell.value
    _excel_exigir(cache is not None and cache.data_type != "e",
                  f"{local}: fórmula com resultado de erro Excel; corrija a origem.")
    _excel_exigir(cache.value is not None,
                  f"{local}: fórmula sem resultado armazenado utilizável; abra no Excel, "
                  "recalcule e salve (ou substitua por valor).")
    return cache.value


def _excel_ler(conteudo, arquivo, cfg, fonte, restantes):
    from openpyxl import load_workbook

    try:
        with ZipFile(BytesIO(conteudo)) as pacote:
            tamanho = sum(info.file_size for info in pacote.infolist())
            _excel_exigir(tamanho <= cfg["max_mb_descompactado"] * 1024**2,
                          f"{fonte}: conteúdo descompactado excede o limite.")
    except BadZipFile:
        raise ExcelQualidadeError(f"{fonte}: arquivo inválido, corrompido ou criptografado.") from None
    wb = load_workbook(BytesIO(conteudo), read_only=True, data_only=False)
    wb_valores = None
    try:
        # Segunda visão do mesmo arquivo: resultados de fórmulas já armazenados (sem recalcular).
        wb_valores = load_workbook(BytesIO(conteudo), read_only=True, data_only=True)
        _excel_exigir(arquivo["aba"] in wb.sheetnames, f"{fonte}: aba não encontrada.")
        sheet = wb[arquivo["aba"]]
        sheet_valores = wb_valores[arquivo["aba"]]
        _excel_exigir((sheet.max_row or 0) <= cfg["max_linhas_por_aba"],
                      f"{fonte}: dimensão de linhas excede o limite.")
        _excel_exigir((sheet.max_column or 0) <= cfg["max_colunas_por_aba"],
                      f"{fonte}: dimensão de colunas excede o limite.")
        linha_header = arquivo.get("linha_cabecalho", 1)
        linhas = sheet.iter_rows(min_row=linha_header)
        linhas_valores = sheet_valores.iter_rows(min_row=linha_header)
        header = next(linhas, None)
        next(linhas_valores, None)
        _excel_exigir(header is not None, f"{fonte}: cabeçalho ausente.")
        _excel_exigir(all(c.data_type not in ("f", "e") for c in header),
                      f"{fonte}: fórmula/erro no cabeçalho.")
        nomes = [str(c.value).strip() if c.value is not None else "" for c in header]
        while nomes and not nomes[-1]:
            nomes.pop()
        _excel_exigir(bool(nomes) and all(nomes), f"{fonte}: cabeçalho vazio/incompleto.")
        _excel_exigir(len({n.lower() for n in nomes}) == len(nomes), f"{fonte}: cabeçalhos duplicados.")
        _excel_exigir(all(unicodedata.is_normalized("NFC", n) for n in nomes),
                      f"{fonte}: cabeçalho com acentuação em forma Unicode decomposta (NFD); "
                      "redigite o nome no Excel.")
        _excel_exigir(set(nomes) == set(cfg["colunas"]), f"{fonte}: layout diferente do configurado.")
        registros = []
        for numero, (cells, caches) in enumerate(zip(linhas, linhas_valores), start=linha_header + 1):
            _excel_exigir(len(cells) == len(caches),
                          f"{fonte}, linha {numero}: leitura inconsistente de fórmulas/resultados.")
            _excel_exigir(numero <= cfg["max_linhas_por_aba"], f"{fonte}: limite de linhas excedido.")
            _excel_exigir(len(cells) <= cfg["max_colunas_por_aba"], f"{fonte}: limite de colunas excedido.")
            if all(_excel_vazio(c.value) for c in cells):
                continue
            _excel_exigir(len(registros) < restantes, "Limite total de registros excedido.")
            _excel_exigir(all(_excel_vazio(c.value) for c in cells[len(nomes):]),
                          f"{fonte}, linha {numero}: dado fora do cabeçalho.")
            # Em read_only, células vazias no fim da linha podem não existir.
            # Toda coluna do cabeçalho é preenchida explicitamente (None quando ausente).
            registro = {}
            for posicao, nome in enumerate(nomes):
                if posicao >= len(cells):
                    registro[nome] = None
                    continue
                cell = cells[posicao]
                valor = _excel_valor_celula(cell, caches[posicao], f"{fonte}, linha {numero}, {nome}")
                regra = cfg.get("formatos", {}).get(nome)
                id_declarado = (
                    regra is not None
                    and regra["tipo"] == "id"
                )

                if nome in cfg["chaves"] and not _excel_vazio(valor):
                    _excel_exigir(
                        isinstance(valor, str) or id_declarado,
                        f"{fonte}, linha {numero}, {nome}: "
                        "chave numérica exige formato id.",
                    )
                try:
                    registro[nome] = _excel_normalizar(valor, regra, wb.epoch)
                except ExcelQualidadeError as erro:
                    raise ExcelQualidadeError(f"{fonte}, linha {numero}, {nome}: {erro}") from None
            registro["_fonte"] = fonte
            registro["_linha"] = str(numero)
            registros.append(registro)
        _excel_exigir(next(linhas, None) is None and next(linhas_valores, None) is None,
                      f"{fonte}: leitura inconsistente de fórmulas/resultados.")
        _excel_exigir(bool(registros) or cfg["permitir_arquivo_vazio"], f"{fonte}: aba vazia bloqueada.")
        return registros
    finally:
        wb.close()
        if wb_valores is not None:
            wb_valores.close()


def _excel_validar(df, schema, chaves, obrigatorias, regras, permitir_vazio, contexto,
                   metadados=False):
    # As colunas de diagnóstico só são aceitas na leitura interna do Excel.
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window

    extras = ["_fonte", "_linha"] if metadados else []
    # Schema analisado uma única vez (no Spark Connect cada acesso pode gerar um Analyze RPC).
    campos = df.schema
    colunas = [f.name for f in campos]
    tipos_origem = {f.name: f.dataType for f in campos}
    _excel_exigir(len({c.lower() for c in colunas}) == len(colunas),
                  f"{contexto}: nomes de colunas duplicados.")
    _excel_exigir(set(colunas) == set(schema) | set(extras),
                  f"{contexto}: colunas recebidas não correspondem ao schema.")

    # Cada verificação é (mensagem, condição de reprovação). Nada é executado no laço:
    # todas são avaliadas juntas em uma única ação Spark ao final.
    verificacoes = []
    expressoes = []
    for nome, tipo in schema.items():
        original = _excel_col(nome)
        origem = tipos_origem[nome]
        texto = F.trim(original.cast("string"))
        normalizado = F.when(texto == "", F.lit(None)).otherwise(texto)
        # Numéricos/datas nativos usam o valor tipado. Strings são normalizadas.
        entrada = normalizado if isinstance(origem, T.StringType) else original
        conv = entrada.try_cast(tipo)
        invalido = entrada.isNotNull() & conv.isNull()

        if isinstance(origem, (T.FloatType, T.DoubleType)):
            invalido = invalido | F.isnan(original) | original.isin(float("inf"), float("-inf"))
        if nome in chaves:
            _excel_exigir(isinstance(origem, T.StringType),
                          f"{contexto}: chave {nome} precisa ser texto.")

        if tipo in ("bigint", "double") or tipo.startswith("decimal"):
            _excel_exigir(isinstance(origem, (T.NumericType, T.StringType)),
                          f"{contexto}: {nome} não possui origem textual/numérica.")
            if isinstance(origem, T.StringType):
                padrao = None
                if tipo == "bigint":
                    padrao = r"^[+-]?\d+$"
                elif tipo.startswith("decimal"):
                    escala = int(re.findall(r"\d+", tipo)[1])
                    padrao = rf"^[+-]?\d+(\.\d{{1,{escala}}})?$" if escala else r"^[+-]?\d+$"
                if padrao:
                    invalido = invalido | (entrada.isNotNull() & ~texto.rlike(padrao))
            elif tipo != "double":
                # Bloqueia truncamento/rounding de valores numéricos tipados.
                invalido = invalido | (original.isNotNull() & (conv.cast(origem) != original))
        if tipo == "date":
            _excel_exigir(isinstance(origem, (T.StringType, T.DateType)),
                          f"{contexto}: {nome} precisa ser date ou texto ISO, sem timestamp.")
            if isinstance(origem, T.StringType):
                invalido = invalido | (entrada.isNotNull() & ~texto.rlike(r"^\d{4}-\d{2}-\d{2}$"))
        if tipo == "boolean":
            _excel_exigir(isinstance(origem, (T.StringType, T.BooleanType)),
                          f"{contexto}: {nome} precisa ser boolean ou texto true/false.")
            if isinstance(origem, T.StringType):
                invalido = invalido | (entrada.isNotNull() & ~texto.isin("true", "false"))
        if tipo == "double":
            invalido = invalido | F.isnan(conv) | conv.isin(float("inf"), float("-inf"))
        verificacoes.append((f"valor inválido/perda de precisão em {nome}", invalido))

        final = normalizado if tipo == "string" else entrada.try_cast(tipo)
        expressoes.append(final.alias(nome))

    # Projeção única: valores tipados + flags de conversão (sem withColumn em laço).
    qtd_conversao = len(verificacoes)
    base = df.select(
        *expressoes, *extras,
        *[F.coalesce(c, F.lit(False)).alias(f"_excel_v{i}") for i, (_, c) in enumerate(verificacoes)],
    )

    # Verificações sobre os valores já tipados.
    for coluna in obrigatorias:
        verificacoes.append((f"campo obrigatório vazio: {coluna}", _excel_col(coluna).isNull()))
    if chaves:
        janela = Window.partitionBy(*[_excel_col(c) for c in chaves])
        verificacoes.append(("chaves duplicadas; corrija a origem",
                             F.count(F.lit(1)).over(janela) > 1))
    if regras:
        # Uma única análise para conferir o tipo de todas as regras.
        tipos_regras = base.select(
            *[F.expr(e).alias(f"_excel_r{i}") for i, e in enumerate(regras.values())]
        ).schema
        for nome, campo in zip(regras, tipos_regras):
            _excel_exigir(isinstance(campo.dataType, T.BooleanType),
                          f"{contexto}: regra {nome} não retorna booleano.")
        for nome, expressao in regras.items():
            verificacoes.append((f"regra violada: {nome}",
                                 ~F.coalesce(F.expr(expressao), F.lit(False))))

    sinalizado = base.select("*", *[
        F.coalesce(c, F.lit(False)).alias(f"_excel_v{i}")
        for i, (_, c) in enumerate(verificacoes) if i >= qtd_conversao
    ])

    # Única ação Spark da validação: contagem e primeira localização de cada falha.
    agregados = [F.count(F.lit(1)).alias("_excel_total")]
    for i in range(len(verificacoes)):
        flag = F.col(f"_excel_v{i}")
        agregados.append(F.count_if(flag).alias(f"_excel_n{i}"))
        if metadados:
            local = F.struct(F.col("_fonte").alias("fonte"),
                             F.col("_linha").cast("int").alias("linha"))
            agregados.append(F.min(F.when(flag, local)).alias(f"_excel_l{i}"))
    resumo = sinalizado.agg(*agregados).first()

    _excel_exigir(permitir_vazio or resumo["_excel_total"] > 0,
                  f"{contexto}: base vazia bloqueada.")
    # Relata todas as falhas de uma vez, sem expor valores de negócio.
    falhas = []
    for i, (mensagem, _) in enumerate(verificacoes):
        qtd = resumo[f"_excel_n{i}"]
        if qtd:
            detalhe = f"{mensagem} ({qtd} linha(s)"
            if metadados:
                local = resumo[f"_excel_l{i}"]
                detalhe += f"; primeira: {local['fonte']}, linha {local['linha']}"
            falhas.append(detalhe + ")")
    _excel_exigir(not falhas, f"{contexto}: " + " | ".join(falhas))
    return df.select(*expressoes)


def validar_dataframe(
    df, *, schema, chaves=None, obrigatorias=None, regras=None,
    permitir_vazio=False, contexto="DataFrame",
):
    """Retorna um novo DataFrame validado e tipado, sem escrever em tabelas.

    schema: dict {coluna: tipo}. Todas as colunas devem constar; nenhuma é ignorada.
    chaves: chave única opcional, textual, não nula.
    obrigatorias: None exige todas; [] permite nulos fora das chaves.
    regras: dict {nome: expressao_SQL_booleana}, com FALSE/NULL reprovados.
    permitir_vazio: False por padrão.
    contexto: nome da etapa para diagnóstico.
    """
    _excel_exigir(type(permitir_vazio) is bool, "permitir_vazio deve ser True/False.")
    chaves, obrigatorias, regras = _excel_contrato(schema, chaves, obrigatorias, regras)
    return _excel_validar(df, schema, chaves, obrigatorias, regras,
                         permitir_vazio, contexto)


def ler_excel_validado(
    *, login, arquivos, schema, chaves=None, obrigatorias=None, regras=None, limites=None,
    formatos=None, retornar_origem=False, localidade=None,
):
    """Lê 1+ Excel de Microsoft 365 e devolve um DataFrame Spark validado.

    arquivos: lista de {drive_id, item_id, aba, linha_cabecalho?}.
    schema: dict {coluna: tipo}; cabeçalhos exatos, em qualquer ordem.
    chaves: opcional. Se informada: colunas textuais, únicas entre todos os arquivos.
            Sem chaves, linhas repetidas (ex.: SKUs repetidos) são aceitas.
    obrigatorias: None exige todas; chaves sempre são obrigatórias.
    regras: dict {nome_regra: SQL booleano} passado pelo consumidor.
    limites: ajustes opcionais para _EXCEL_PADROES.
    formatos: opcional {coluna: {"tipo": id|data|periodo|inteiro|decimal|moeda, ...}}.
              Interpreta códigos numéricos, datas em texto/série e números "1.234,56" / "R$".
              Chave numérica no Excel exige formato id.
    localidade: opcional "pt_BR" | "en_US". Define separadores e ordem dia/mês das
                colunas com formato data, periodo, decimal ou moeda (cada coluna pode
                declarar a sua). Sem localidade, o comportamento é o de sempre.
    retornar_origem: True devolve (df, origem), com nome/eTag/IDs de cada arquivo lido
                     (útil para registrar_snapshot(origem=...)).

    login: credencial retornada por conectar_sharepoint. Requer uma sessão Spark.
    Não grava dados nem devolve credenciais/metadados internos.
    """
    from pyspark.sql import types as T
    spark = _sessao_spark()

    chaves, obrigatorias, regras = _excel_contrato(schema, chaves, obrigatorias, regras)
    cfg = _excel_config_leitura(arquivos, schema, chaves, limites, formatos, localidade)
    graph = _ExcelGraph(login)
    try:
        metadados = [graph.metadata(a) for a in cfg["arquivos"]]
        registros = []
        for indice, (arquivo, meta) in enumerate(zip(cfg["arquivos"], metadados), start=1):
            limite = cfg["max_mb_por_arquivo"] * 1024**2
            _excel_exigir(meta["size"] <= limite, f"Fonte {indice}: arquivo excede o limite.")
            conteudo = graph.baixar(arquivo, limite)
            _excel_exigir(len(conteudo) == meta["size"],
                          f"Fonte {indice}: tamanho mudou/download incompleto.")
            fonte = f"Fonte {indice}: {meta['name']} / {arquivo['aba']}"
            registros.extend(_excel_ler(conteudo, arquivo, cfg, fonte,
                                        cfg["max_linhas_total"] - len(registros)))
        raw_schema = T.StructType([
            T.StructField(c, T.StringType(), True)
            for c in list(schema) + ["_fonte", "_linha"]
        ])
        bruto = spark.createDataFrame(registros, raw_schema)
        validado = _excel_validar(
            bruto, schema, chaves, obrigatorias, regras,
            cfg["permitir_base_vazia"], "Leitura Excel", metadados=True,
        )
        for arquivo, anterior in zip(cfg["arquivos"], metadados):
            atual = graph.metadata(arquivo)
            _excel_exigir(atual["eTag"] == anterior["eTag"],
                          "Uma fonte mudou durante a leitura/validação; execute novamente.")
        if not retornar_origem:
            return validado
        origem = [
            {"drive_id": a["drive_id"], "item_id": a["item_id"], "aba": a["aba"],
             "nome": m.get("name"), "eTag": m.get("eTag"),
             "modificado_em": m.get("lastModifiedDateTime"), "url": m.get("webUrl")}
            for a, m in zip(cfg["arquivos"], metadados)
        ]
        return validado, origem
    finally:
        graph.close()


_EXCEL_HISTORICO = {
    "retencao_dias": 730,   # 2 anos
    "max_tentativas": 6,    # tentativas em caso de conflito de concorrência
}


_EXCEL_COLUNAS_HISTORICO = {
    "id_fonte": "string", "versao": "bigint", "hash_snapshot": "string",
    "hash_contrato": "string", "contrato": "string", "hash_linha": "string",
    "ocorrencia": "int", "dados": "variant", "origem": "string",
    "job_id": "string", "run_id": "string", "notebook": "string",
    "usuario": "string", "carregado_em": "timestamp",
}


def _excel_preparar_tabela(tabela, *, criar=False):
    spark = _sessao_spark()

    _excel_exigir(isinstance(tabela, str) and re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2}", tabela),
        "Use tabela, schema.tabela ou catalogo.schema.tabela com letras ASCII, números e sublinhado.")
    nome = ".".join(f"`{parte}`" for parte in tabela.split("."))
    if criar and not spark.catalog.tableExists(nome):
        ddl = ", ".join(f"`{coluna}` {tipo}" for coluna, tipo in _EXCEL_COLUNAS_HISTORICO.items())
        spark.sql(f"CREATE TABLE IF NOT EXISTS {nome} ({ddl}) USING DELTA")
    detalhe = spark.sql(f"DESCRIBE DETAIL {nome}").first()
    _excel_exigir(detalhe["format"] == "delta", f"{nome}: o histórico exige uma tabela Delta.")
    atual = {f.name: f.dataType.simpleString() for f in spark.table(nome).schema.fields}
    _excel_exigir(atual == _EXCEL_COLUNAS_HISTORICO,
                  f"{nome}: schema incompatível. Esperado {_EXCEL_COLUNAS_HISTORICO}. Encontrado {atual}.")
    return nome


class _ExcelConflito(RuntimeError):
    """Versão ocupada por outro escritor; a operação deve ser repetida."""


def _excel_validar_id_fonte(id_fonte):
    _excel_exigir(isinstance(id_fonte, str) and re.fullmatch(r"[a-z][a-z0-9_]{2,99}", id_fonte),
                  "id_fonte: 3-100 caracteres minúsculos, números ou sublinhado, iniciando por letra.")
    return id_fonte


def _excel_contrato_json(schema, chaves, obrigatorias, regras):
    contrato = {"schema": dict(sorted(schema.items())), "chaves": list(chaves),
                "obrigatorias": list(obrigatorias), "regras": dict(sorted(regras.items()))}
    texto = json.dumps(contrato, sort_keys=True, ensure_ascii=False)
    return texto, hashlib.sha256(texto.encode("utf-8")).hexdigest()


def _excel_execucao():
    # Identificação do Job/notebook para auditoria; ausência não impede o registro.
    info = {"job_id": None, "run_id": None, "notebook": None}
    try:
        from databricks.sdk.runtime import dbutils

        ctx = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson())
        attrs = ctx.get("attributes", ctx)
        info["job_id"] = attrs.get("jobId") or None
        info["run_id"] = (attrs.get("multitaskParentRunId") or attrs.get("currentRunId")
                          or attrs.get("runId") or None)
        info["notebook"] = attrs.get("notebook_path") or None
    except Exception:
        pass
    return {k: (str(v) if v is not None else None) for k, v in info.items()}


def _excel_conflito(erro):
    if isinstance(erro, _ExcelConflito):
        return True
    texto = type(erro).__name__ + " " + str(erro)[:500]
    return any(m in texto for m in ("ConcurrentAppend", "ConcurrentDelete", "ConcurrentTransaction",
                                     "DELTA_CONCURRENT", "MetadataChanged", "ProtocolChanged"))


def _excel_repetir(acao):
    tentativas = _EXCEL_HISTORICO["max_tentativas"]
    for tentativa in range(1, tentativas + 1):
        try:
            return acao()
        except Exception as erro:
            if not _excel_conflito(erro) or tentativa == tentativas:
                raise
            time.sleep(min(30, 2 ** tentativa) * random.uniform(0.5, 1.0))


def _excel_historico(tabela, id_fonte):
    from pyspark.sql import functions as F
    spark = _sessao_spark()

    return spark.table(tabela).where(F.col("id_fonte") == id_fonte)


def _excel_expurgar(tabela, id_fonte, versao_atual):
    # Remove versões com mais de 2 anos, nunca a versão atual da fonte.
    spark = _sessao_spark()

    def acao():
        spark = _sessao_spark()

        return spark.sql(
            f"DELETE FROM {tabela} "
            "WHERE id_fonte = :id AND versao < :versao "
            "AND carregado_em < current_timestamp() - make_dt_interval(:dias)",
            args={"id": id_fonte, "versao": versao_atual, "dias": _EXCEL_HISTORICO["retencao_dias"]},
        ).first()["num_affected_rows"]
    try:
        return _excel_repetir(acao)
    except Exception as erro:
        return f"não executado ({type(erro).__name__}); será refeito no próximo registro"


def registrar_snapshot(
    df, *, tabela, id_fonte, schema, chaves=None, obrigatorias=None, regras=None,
    origem=None, permitir_vazio=False,
):
    """Registra a planilha inteira como nova versão de id_fonte, se o conteúdo mudou.

    df: DataFrame (normalmente o retorno de ler_excel_validado). É revalidado aqui.
    id_fonte: nome lógico e estável da planilha; independe do local do arquivo.
    schema/chaves/obrigatorias/regras: mesmo contrato de validar_dataframe.
    origem: dict/list opcional para auditoria (ex.: retorno de ler_excel_validado(retornar_origem=True)).

    Retorna dict: status ('nova_versao' | 'identico'), versao, linhas, linhas_ineditas, expurgo.
    linhas_ineditas = linhas desta versão que não existiam em nenhuma versão anterior retida.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from delta.tables import DeltaTable
    spark = _sessao_spark()

    _excel_validar_id_fonte(id_fonte)
    _excel_exigir(type(permitir_vazio) is bool, "permitir_vazio deve ser True/False.")
    _excel_exigir(origem is None or isinstance(origem, (dict, list)), "origem deve ser dict ou list.")
    chaves_n, obrig_n, regras_n = _excel_contrato(schema, chaves, obrigatorias, regras)
    validado = _excel_validar(df, schema, chaves_n, obrig_n, regras_n, permitir_vazio,
                              f"Snapshot {id_fonte}")
    contrato, hash_contrato = _excel_contrato_json(schema, chaves_n, obrig_n, regras_n)
    origem_json = (json.dumps(origem, sort_keys=True, ensure_ascii=False, default=str)
                   if origem is not None else None)
    execucao = _excel_execucao()
    tabela = _excel_preparar_tabela(tabela, criar=True)

    # Impressão digital da linha: colunas em ordem alfabética e nulos explícitos,
    # para não depender da ordem das colunas no Excel/schema.
    estrutura_hash = F.struct(*[_excel_col(c) for c in sorted(schema)])
    linhas = (
        validado.select(
            F.sha2(F.to_json(estrutura_hash, {"ignoreNullFields": "false"}), 256).alias("hash_linha"),
            F.to_variant_object(F.struct(*[_excel_col(c) for c in schema])).alias("dados"),
        )
        # Linhas idênticas legítimas viram ocorrências 1, 2, 3...
        .select("*", F.row_number().over(Window.partitionBy("hash_linha").orderBy("hash_linha"))
                .cast("int").alias("ocorrencia"))
    )
    resumo = linhas.agg(
        F.count(F.lit(1)).alias("total"),
        F.sha2(F.concat_ws(",", F.array_sort(F.collect_list(
            F.concat_ws(":", "hash_linha", F.col("ocorrencia").cast("string"))))), 256).alias("conteudo"),
    ).first()
    total = resumo["total"]
    hash_snapshot = hashlib.sha256(f"{hash_contrato}|{resumo['conteudo']}".encode()).hexdigest()
    if total == 0:
        # Snapshot vazio permitido: uma linha marcadora sem dados.
        linhas = spark.range(1).select(F.lit(None).cast("string").alias("hash_linha"),
                                       F.expr("CAST(NULL AS VARIANT)").alias("dados"),
                                       F.lit(0).alias("ocorrencia"))
    esperado = max(total, 1)

    def tentativa():
        from pyspark.sql import functions as F
        from delta.tables import DeltaTable
        spark = _sessao_spark()

        ultimo = _excel_historico(tabela, id_fonte).agg(
            F.max("versao").alias("versao"), F.max_by("hash_snapshot", "versao").alias("hash")).first()
        if ultimo["versao"] is not None and ultimo["hash"] == hash_snapshot:
            return {"status": "identico", "versao": ultimo["versao"]}
        versao = (ultimo["versao"] or 0) + 1
        fonte = linhas.select(
            F.lit(id_fonte).alias("id_fonte"), F.lit(versao).cast("bigint").alias("versao"),
            F.lit(hash_snapshot).alias("hash_snapshot"), F.lit(hash_contrato).alias("hash_contrato"),
            F.lit(contrato).alias("contrato"), "hash_linha", "ocorrencia", "dados",
            F.lit(origem_json).cast("string").alias("origem"),
            F.lit(execucao["job_id"]).cast("string").alias("job_id"),
            F.lit(execucao["run_id"]).cast("string").alias("run_id"),
            F.lit(execucao["notebook"]).cast("string").alias("notebook"),
            F.expr("current_user()").alias("usuario"),
            F.current_timestamp().alias("carregado_em"),
        )
        # MERGE atômico: se outro escritor gravar a mesma versão ao mesmo tempo, o Delta
        # rejeita um dos dois com conflito e a tentativa é refeita com a tabela atualizada.
        (DeltaTable.forName(spark, tabela).alias("t")
         .merge(fonte.alias("s"), "t.id_fonte = s.id_fonte AND t.versao = s.versao")
         .whenNotMatchedInsertAll().execute())
        conferencia = _excel_historico(tabela, id_fonte).where(F.col("versao") == versao).agg(
            F.count_if(F.col("hash_snapshot") == hash_snapshot).alias("meus"),
            F.count(F.lit(1)).alias("todos")).first()
        if conferencia["meus"] == 0:
            raise _ExcelConflito("Versão ocupada por outro escritor.")
        _excel_exigir(conferencia["meus"] == conferencia["todos"] == esperado,
                      f"Snapshot {id_fonte} v{versao}: conferência pós-gravação falhou; verifique a tabela.")
        return {"status": "nova_versao", "versao": versao}

    resultado = _excel_repetir(tentativa)
    resultado.update({"id_fonte": id_fonte, "linhas": total, "hash_snapshot": hash_snapshot,
                      "linhas_ineditas": 0, "expurgo": 0})
    if resultado["status"] == "nova_versao":
        versao = resultado["versao"]
        historico = _excel_historico(tabela, id_fonte).where(F.col("hash_linha").isNotNull())
        atuais = historico.where(F.col("versao") == versao).select("hash_linha", "ocorrencia")
        anteriores = historico.where(F.col("versao") < versao).select("hash_linha", "ocorrencia")
        resultado["linhas_ineditas"] = atuais.join(anteriores, ["hash_linha", "ocorrencia"],
                                                   "left_anti").count()
        resultado["expurgo"] = _excel_expurgar(tabela, id_fonte, versao)
    return resultado


def _excel_contratos_compativeis(tabela, id_fonte, schema):
    # Versões cujo schema registrado tem todas as colunas pedidas com o mesmo tipo.
    contratos = _excel_historico(tabela, id_fonte).select("hash_contrato", "contrato").distinct().collect()
    return [c["hash_contrato"] for c in contratos
            if all(json.loads(c["contrato"])["schema"].get(n) == t for n, t in schema.items())]


def _excel_tipar(df, schema, extras=()):
    from pyspark.sql import functions as F

    return df.select(*[F.variant_get("dados", _excel_caminho_variant(c), t).alias(c) for c, t in schema.items()],
                     *extras)


def listar_snapshots(*, tabela, id_fonte):
    """Versões retidas de id_fonte com quantidade de linhas e auditoria (origem, Job, usuário)."""
    from pyspark.sql import functions as F

    _excel_validar_id_fonte(id_fonte)
    tabela = _excel_preparar_tabela(tabela)
    return (_excel_historico(tabela, id_fonte).groupBy("versao").agg(
                F.min("carregado_em").alias("carregado_em"),
                F.count("hash_linha").alias("linhas"),
                F.first("hash_snapshot").alias("hash_snapshot"),
                F.first("hash_contrato").alias("hash_contrato"),
                F.first("origem").alias("origem"),
                F.first("job_id").alias("job_id"), F.first("run_id").alias("run_id"),
                F.first("notebook").alias("notebook"), F.first("usuario").alias("usuario"))
            .orderBy("versao"))


def ler_snapshot(*, tabela, id_fonte, schema, versao=None):
    """Devolve uma versão (padrão: a última) exatamente como foi carregada, já tipada."""
    from pyspark.sql import functions as F

    _excel_validar_id_fonte(id_fonte)
    tabela = _excel_preparar_tabela(tabela)
    _excel_contrato(schema, None, [], None)
    _excel_exigir(versao is None or (type(versao) is int and versao >= 1), "versao deve ser inteiro >= 1.")
    historico = _excel_historico(tabela, id_fonte)
    if versao is not None:
        historico = historico.where(F.col("versao") == versao)
    info = historico.agg(F.max("versao").alias("versao"),
                         F.max_by("hash_contrato", "versao").alias("contrato")).first()
    _excel_exigir(info["versao"] is not None, f"{id_fonte}: versão não encontrada.")
    _excel_exigir(info["contrato"] in _excel_contratos_compativeis(tabela, id_fonte, schema),
                  f"{id_fonte} v{info['versao']}: schema pedido difere do registrado.")
    linhas = _excel_historico(tabela, id_fonte).where(
        (F.col("versao") == info["versao"]) & F.col("hash_linha").isNotNull())
    return _excel_tipar(linhas, schema)


def ler_acumulado(*, tabela, id_fonte, schema):
    """Todas as linhas já vistas em versões retidas, sem duplicar linhas idênticas.

    Linha excluída do Excel continua aqui. Mesmo conteúdo = uma linha só;
    conteúdo diferente (mesmo com o mesmo código) = outra linha.
    Colunas extras: _primeira_versao, _primeira_carga, _ultima_versao, _na_versao_atual.
    """
    from pyspark.sql import functions as F

    _excel_validar_id_fonte(id_fonte)
    tabela = _excel_preparar_tabela(tabela)
    _excel_contrato(schema, None, [], None)
    compativeis = _excel_contratos_compativeis(tabela, id_fonte, schema)
    _excel_exigir(bool(compativeis), f"{id_fonte}: nenhuma versão com o schema pedido.")
    historico = _excel_historico(tabela, id_fonte)
    atual = historico.agg(F.max("versao")).first()[0]
    vistos = (historico
              .where(F.col("hash_linha").isNotNull() & F.col("hash_contrato").isin(compativeis))
              .groupBy("hash_linha", "ocorrencia").agg(
                  F.min_by("dados", "versao").alias("dados"),
                  F.min("versao").alias("_primeira_versao"),
                  F.min("carregado_em").alias("_primeira_carga"),
                  F.max("versao").alias("_ultima_versao")))
    return _excel_tipar(vistos, schema, extras=[
        "_primeira_versao", "_primeira_carga", "_ultima_versao",
        (F.col("_ultima_versao") == F.lit(atual)).alias("_na_versao_atual")])
