"""Comportamentos atuais que precisam continuar iguais (não exigem Spark).

Estes testes protegem cargas existentes: uma mudança que altere qualquer
resultado abaixo muda o contrato da biblioteca e precisa ser opt-in.
"""

from datetime import date, datetime
from io import BytesIO

import pytest
from openpyxl import Workbook

from PointBricks._core import (
    ExcelQualidadeError,
    _excel_compilar_formatos,
    _excel_config_leitura,
    _excel_ler,
    _excel_texto,
    _fmt_data,
    _fmt_id,
    _fmt_numero,
)


def regra(tipo_schema, **formato):
    return _excel_compilar_formatos({"C": formato}, {"C": tipo_schema})["C"]


def xlsx(linhas):
    wb = Workbook()
    ws = wb.active
    ws.title = "Dados"
    for linha in linhas:
        ws.append(linha)
    buffer = BytesIO()
    wb.save(buffer)
    return buffer.getvalue()


def ler(conteudo, schema, formatos=None):
    arquivo = {"drive_id": "d", "item_id": "i", "aba": "Dados"}
    cfg = _excel_config_leitura([arquivo], schema, [], None, formatos)
    return _excel_ler(conteudo, arquivo, cfg, "Fonte 1", 1000)


# --- IDs: representação textual preservada ----------------------------------

@pytest.mark.parametrize("valor, esperado", [
    (123, "123"),
    ("123", "123"),
    (" 123 ", "123"),
    ("123,0", "123,0"),
    ("3.1", "3.1"),
    ("A-10", "A-10"),
    (-5, "-5"),
])
def test_id_padrao(valor, esperado):
    assert _fmt_id(valor, regra("string", tipo="id")) == esperado


def test_id_largura_numerica():
    r = regra("string", tipo="id", largura_numerica=6)
    assert _fmt_id(123, r) == _fmt_id("123", r) == "000123"


def test_id_texto_decimal_inteiro():
    r = regra("string", tipo="id", texto_decimal_inteiro=True)
    assert _fmt_id("123,0", r) == "123"
    assert _fmt_id("123.00", r) == "123"
    with pytest.raises(ExcelQualidadeError):
        _fmt_id("123,5", r)


# --- Números e moeda ---------------------------------------------------------

@pytest.mark.parametrize("texto, esperado", [
    ("R$ 1.234,56", "1234.56"),
    ("R$1.234,56", "1234.56"),
    ("1.234,56", "1234.56"),
    ("-R$ 10,00", "-10"),
    ("R$ -10,00", "-10"),
    ("R$ - 10,00", "-10"),
    ("- 10,00", "-10"),
    ("- R$ 10,00", "-10"),
    ("R$ 10,00", "10"),
    ("1500", "1500"),
])
def test_moeda_aceita(texto, esperado):
    assert _fmt_numero(texto, regra("decimal(18,2)", tipo="moeda")) == esperado


@pytest.mark.parametrize("texto", ["1.2345", "abc", "R$", "1,2,3"])
def test_moeda_rejeita(texto):
    with pytest.raises(ExcelQualidadeError):
        _fmt_numero(texto, regra("decimal(18,2)", tipo="moeda"))


def test_decimal_com_separador_de_milhar_espaco():
    r = regra("double", tipo="decimal", separador_milhar=" ")
    assert _fmt_numero("1 234,5", r) == "1234.5"


def test_inteiro_rejeita_fracao():
    r = regra("bigint", tipo="inteiro")
    assert _fmt_numero("1.234", r) == "1234"
    with pytest.raises(ExcelQualidadeError):
        _fmt_numero("10,5", r)


def test_numero_nativo():
    r = regra("decimal(18,2)", tipo="moeda")
    assert _fmt_numero(10.5, r) == "10.5"
    assert _fmt_numero(7, r) == "7"


# --- Datas -------------------------------------------------------------------

def test_data_serial_1900():
    assert _fmt_data(45000, regra("date", tipo="data")) == "2023-03-15"


def test_data_sem_corte_temporal():
    # O consumidor declara o tipo da coluna; não há ano mínimo por padrão.
    assert _fmt_data(2024, regra("date", tipo="data")) == "1905-07-16"


@pytest.mark.parametrize("valor, esperado", [
    ("2024-03-15", "2024-03-15"),
    ("15/03/2024", "2024-03-15"),
    (datetime(2024, 3, 15), "2024-03-15"),
    (date(2024, 3, 15), "2024-03-15"),
])
def test_data_formatos_padrao(valor, esperado):
    assert _fmt_data(valor, regra("date", tipo="data")) == esperado


def test_data_com_horario_rejeitada_por_padrao():
    with pytest.raises(ExcelQualidadeError):
        _fmt_data(datetime(2024, 3, 15, 10, 30), regra("date", tipo="data"))


def test_periodo_vira_dia_1():
    r = regra("date", tipo="periodo")
    assert _fmt_data("03/2024", r) == "2024-03-01"
    assert _fmt_data("2024-03-15", r) == "2024-03-01"


# --- Leitura sem formato declarado -------------------------------------------

def test_texto_canonico_de_valores_nativos():
    assert _excel_texto(0.1 + 0.2) == "0.3"
    assert _excel_texto(True) == "true"
    assert _excel_texto(date(2024, 1, 5)) == "2024-01-05"


def test_numero_em_coluna_texto_sem_formato():
    assert ler(xlsx([["CEP"], [1234]]), {"CEP": "string"})[0]["CEP"] == "1234"


def test_leitura_com_formatos():
    registros = ler(
        xlsx([["Cod", "Valor", "Data"], [7, "R$ 1.234,56", 45000]]),
        {"Cod": "string", "Valor": "decimal(18,2)", "Data": "date"},
        {"Cod": {"tipo": "id", "largura_numerica": 3},
         "Valor": {"tipo": "moeda"}, "Data": {"tipo": "data"}},
    )
    assert registros[0] == {"Cod": "007", "Valor": "1234.56", "Data": "2023-03-15",
                            "_fonte": "Fonte 1", "_linha": "2"}
