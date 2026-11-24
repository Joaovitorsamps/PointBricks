"""Localidade declarativa (pt_BR, en_US) para datas e números decimais em texto."""

from datetime import date
from io import BytesIO

import pytest
from openpyxl import Workbook

from PointBricks._core import (
    ExcelQualidadeError,
    _excel_compilar_formatos,
    _excel_config_leitura,
    _excel_ler,
    _fmt_data,
    _fmt_id,
    _fmt_numero,
)


def regra(tipo_schema, localidade=None, **formato):
    return _excel_compilar_formatos({"C": formato}, {"C": tipo_schema}, localidade)["C"]


# --- A mesma informação em localidades diferentes dá o mesmo valor ---------------

@pytest.mark.parametrize("localidade, texto", [
    ("pt_BR", "1.234,56"),
    ("en_US", "1,234.56"),
    ("pt_BR", "1234,56"),
    ("en_US", "1234.56"),
])
def test_decimal_mesmo_valor(localidade, texto):
    assert _fmt_numero(texto, regra("double", localidade, tipo="decimal")) == "1234.56"


@pytest.mark.parametrize("localidade, texto", [
    ("pt_BR", "10/12/2024"),
    ("en_US", "12/10/2024"),
    ("pt_BR", "2024-12-10"),
    ("en_US", "2024-12-10"),
])
def test_data_mesma_data(localidade, texto):
    assert _fmt_data(texto, regra("date", localidade, tipo="data")) == "2024-12-10"


@pytest.mark.parametrize("localidade, texto", [
    ("pt_BR", "12/2024"),
    ("en_US", "12/2024"),
    ("pt_BR", "10/12/2024"),
    ("en_US", "12/10/2024"),
])
def test_periodo_mesmo_mes(localidade, texto):
    assert _fmt_data(texto, regra("date", localidade, tipo="periodo")) == "2024-12-01"


@pytest.mark.parametrize("localidade, texto", [
    ("pt_BR", "R$ 1.234,56"),
    ("en_US", "R$ 1,234.56"),
    ("pt_BR", "-R$ 1.234,56"),
    ("en_US", "R$ - 1,234.56"),
])
def test_moeda_mesmo_valor(localidade, texto):
    esperado = "-1234.56" if "-" in texto else "1234.56"
    assert _fmt_numero(texto, regra("decimal(18,2)", localidade, tipo="moeda")) == esperado


def test_moeda_com_simbolo_proprio():
    r = regra("decimal(18,2)", "en_US", tipo="moeda", simbolo="$")
    assert _fmt_numero("-$1,234.56", r) == "-1234.56"
    assert _fmt_numero("$ 10.00", r) == "10"


# --- O que não bate com a localidade declarada continua sendo erro ---------------

@pytest.mark.parametrize("localidade, texto", [
    ("en_US", "1.234,56"),
    ("pt_BR", "1,234.56"),
    ("pt_BR", "12 34"),
    ("pt_BR", "1 234,56"),
    ("en_US", "1 234.56"),
])
def test_decimal_fora_da_localidade(localidade, texto):
    with pytest.raises(ExcelQualidadeError):
        _fmt_numero(texto, regra("double", localidade, tipo="decimal"))


@pytest.mark.parametrize("localidade, texto", [
    ("en_US", "25/12/2024"),
    ("pt_BR", "12/25/2024"),
])
def test_data_fora_da_localidade(localidade, texto):
    with pytest.raises(ExcelQualidadeError):
        _fmt_data(texto, regra("date", localidade, tipo="data"))


def test_espacos_entre_sinal_simbolo_e_valor_continuam_aceitos():
    r = regra("decimal(18,2)", "pt_BR", tipo="moeda")
    assert _fmt_numero("- 10,00", r) == "-10"
    assert _fmt_numero("R$ - 10,00", r) == "-10"
    assert _fmt_numero("10,00 R$", r) == "10"


# --- Valores nativos do Excel não dependem de localidade --------------------------

@pytest.mark.parametrize("localidade", ["pt_BR", "en_US"])
def test_valores_nativos_ignoram_localidade(localidade):
    assert _fmt_numero(1234.56, regra("double", localidade, tipo="decimal")) == "1234.56"
    assert _fmt_data(date(2024, 12, 10), regra("date", localidade, tipo="data")) == "2024-12-10"
    assert _fmt_data(45000, regra("date", localidade, tipo="data")) == "2023-03-15"


# --- Precedência e escopo ---------------------------------------------------------

def test_localidade_da_coluna_vence_a_geral():
    r = _excel_compilar_formatos(
        {"V": {"tipo": "decimal", "localidade": "en_US"}}, {"V": "double"}, "pt_BR")["V"]
    assert _fmt_numero("1,234.56", r) == "1234.56"


def test_opcao_explicita_vence_a_localidade():
    r = regra("double", "en_US", tipo="decimal", separador_decimal=",")
    assert _fmt_numero("1234,5", r) == "1234.5"
    r = regra("date", "en_US", tipo="data", formatos_texto=["%d.%m.%Y"])
    assert _fmt_data("10.12.2024", r) == "2024-12-10"


def test_localidade_geral_nao_afeta_inteiro_nem_id():
    inteiro = _excel_compilar_formatos({"N": {"tipo": "inteiro"}}, {"N": "bigint"}, "en_US")["N"]
    assert _fmt_numero("1.234", inteiro) == "1234"
    ident = _excel_compilar_formatos({"I": {"tipo": "id"}}, {"I": "string"}, "en_US")["I"]
    assert _fmt_id("1,5", ident) == "1,5"


@pytest.mark.parametrize("tipo, schema", [("inteiro", "bigint"), ("id", "string")])
def test_localidade_por_coluna_rejeitada_em_inteiro_e_id(tipo, schema):
    with pytest.raises(ExcelQualidadeError, match="localidade se aplica"):
        _excel_compilar_formatos({"C": {"tipo": tipo, "localidade": "pt_BR"}}, {"C": schema})


@pytest.mark.parametrize("valor", ["pt-BR", "PT_BR", "fr_FR", 1])
def test_localidade_invalida(valor):
    with pytest.raises(ExcelQualidadeError, match="localidade"):
        _excel_compilar_formatos({"V": {"tipo": "decimal"}}, {"V": "double"}, valor)
    with pytest.raises(ExcelQualidadeError, match="localidade"):
        _excel_compilar_formatos({"V": {"tipo": "decimal", "localidade": valor}}, {"V": "double"})


def test_localidade_invalida_mesmo_sem_formatos():
    arquivo = {"drive_id": "d", "item_id": "i", "aba": "Dados"}
    with pytest.raises(ExcelQualidadeError, match="localidade"):
        _excel_config_leitura([arquivo], {"A": "string"}, [], None, None, "xx")


def test_sem_localidade_nada_muda():
    sem = regra("double", None, tipo="decimal")
    assert _fmt_numero("1 234,56", sem) == "1234.56"
    assert _fmt_data("10/12/2024", regra("date", None, tipo="data")) == "2024-12-10"


# --- Leitura de planilha -------------------------------------------------------------

def test_leitura_com_localidade_geral():
    wb = Workbook()
    ws = wb.active
    ws.title = "Dados"
    ws.append(["Data", "Valor", "Cod"])
    ws.append(["12/10/2024", "$1,234.56", "1,5"])
    buffer = BytesIO()
    wb.save(buffer)
    arquivo = {"drive_id": "d", "item_id": "i", "aba": "Dados"}
    cfg = _excel_config_leitura(
        [arquivo], {"Data": "date", "Valor": "decimal(18,2)", "Cod": "string"}, [], None,
        {"Data": {"tipo": "data"}, "Valor": {"tipo": "moeda", "simbolo": "$"},
         "Cod": {"tipo": "id"}},
        "en_US",
    )
    registro = _excel_ler(buffer.getvalue(), arquivo, cfg, "Fonte 1", 1000)[0]
    assert (registro["Data"], registro["Valor"], registro["Cod"]) == ("2024-12-10", "1234.56", "1,5")
