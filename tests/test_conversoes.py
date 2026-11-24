"""Testes das conversões de células (não exigem Spark).

Cada teste marcado com um código (B1, B2...) cobre um erro em que um valor
errado era aceito sem aviso.
"""

from datetime import date, datetime
from io import BytesIO

import pytest
from openpyxl import Workbook
from openpyxl.utils.datetime import CALENDAR_MAC_1904

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


# --- IDs -------------------------------------------------------------------

def test_b1_id_com_separador_de_milhar_e_rejeitado():
    r = regra("string", tipo="id", texto_decimal_inteiro=True)
    with pytest.raises(ExcelQualidadeError, match="ambíguo"):
        _fmt_id("1.000", r)


def test_b1_id_com_fracao_zero_ainda_e_convertido():
    r = regra("string", tipo="id", texto_decimal_inteiro=True)
    assert _fmt_id("123,0", r) == "123"
    assert _fmt_id("123.00", r) == "123"


def test_b7_id_textual_com_fracao_zero_e_rejeitado_por_padrao():
    r = regra("string", tipo="id")
    with pytest.raises(ExcelQualidadeError, match="texto_decimal_inteiro"):
        _fmt_id("123,0", r)


def test_b7_codigos_textuais_com_ponto_continuam_validos():
    r = regra("string", tipo="id")
    assert _fmt_id("3.1", r) == "3.1"
    assert _fmt_id("A-10", r) == "A-10"


def test_id_numerico_e_textual_geram_a_mesma_chave():
    r = regra("string", tipo="id", largura_numerica=6)
    assert _fmt_id(123, r) == _fmt_id("123", r) == "000123"


# --- Números e moeda ---------------------------------------------------------

def test_b2_simbolo_no_meio_do_numero_e_rejeitado():
    r = regra("decimal(18,2)", tipo="moeda")
    with pytest.raises(ExcelQualidadeError):
        _fmt_numero("10R$5", r)


def test_b2_espacos_dentro_do_numero_sao_rejeitados():
    r = regra("decimal(18,2)", tipo="moeda")
    with pytest.raises(ExcelQualidadeError):
        _fmt_numero("12 34", r)
    with pytest.raises(ExcelQualidadeError):
        _fmt_numero("1 234,56", r)


@pytest.mark.parametrize("texto, esperado", [
    ("R$ 1.234,56", "1234.56"),
    ("R$1.234,56", "1234.56"),
    ("1.234,56", "1234.56"),
    ("-R$ 10,00", "-10"),
    ("R$ -10,00", "-10"),
    ("10,50 R$", "10.5"),
    ("R$ 10,00", "10"),
])
def test_b2_formatos_validos_de_moeda(texto, esperado):
    r = regra("decimal(18,2)", tipo="moeda")
    assert _fmt_numero(texto, r) == esperado


def test_separador_de_milhar_espaco_continua_aceito():
    r = regra("double", tipo="decimal", separador_milhar=" ")
    assert _fmt_numero("1 234,5", r) == "1234.5"


def test_b3_milhar_ambiguo_aceito_por_padrao():
    r = regra("double", tipo="decimal")
    assert _fmt_numero("1.500", r) == "1500"


def test_b3_milhar_ambiguo_rejeitado_quando_ligado():
    r = regra("double", tipo="decimal", rejeitar_milhar_ambiguo=True)
    with pytest.raises(ExcelQualidadeError, match="ambíguo"):
        _fmt_numero("1.500", r)
    assert _fmt_numero("1.500,00", r) == "1500"
    assert _fmt_numero("1.234.567", r) == "1234567"
    assert _fmt_numero("1500", r) == "1500"


def test_rejeitar_milhar_ambiguo_deve_ser_booleano():
    with pytest.raises(ExcelQualidadeError):
        regra("double", tipo="decimal", rejeitar_milhar_ambiguo="sim")


# --- Datas -------------------------------------------------------------------

def test_b4_numero_pequeno_em_coluna_data_e_rejeitado():
    r = regra("date", tipo="data")
    with pytest.raises(ExcelQualidadeError, match="ano_minimo_serie"):
        _fmt_data(2024, r)
    with pytest.raises(ExcelQualidadeError):
        _fmt_data(1, r)


def test_b4_serie_normal_continua_valida():
    r = regra("date", tipo="data")
    assert _fmt_data(45000, r) == "2023-03-15"


def test_b4_ano_minimo_configuravel():
    r = regra("date", tipo="data", ano_minimo_serie=1900)
    assert _fmt_data(2024, r) == "1905-07-16"


def test_b4_datas_nativas_antigas_nao_sao_afetadas():
    r = regra("date", tipo="data")
    assert _fmt_data(datetime(1900, 5, 1), r) == "1900-05-01"


def test_ano_minimo_serie_invalido():
    with pytest.raises(ExcelQualidadeError):
        regra("date", tipo="data", ano_minimo_serie="1950")


def test_b5_epoch_1904_em_fmt_data():
    r = regra("date", tipo="data")
    assert _fmt_data(45000, r, epoch=CALENDAR_MAC_1904) == "2027-03-16"


# --- Leitura de planilha (_excel_ler) ----------------------------------------

def _xlsx(linhas, epoch=None):
    wb = Workbook()
    if epoch is not None:
        wb.epoch = epoch
    ws = wb.active
    ws.title = "Dados"
    for linha in linhas:
        ws.append(linha)
    buffer = BytesIO()
    wb.save(buffer)
    return buffer.getvalue()


def _ler(conteudo, schema, formatos=None, limites=None):
    arquivo = {"drive_id": "d", "item_id": "i", "aba": "Dados"}
    cfg = _excel_config_leitura([arquivo], schema, [], limites, formatos)
    return _excel_ler(conteudo, arquivo, cfg, "Fonte 1", 1000)


def test_b5_planilha_1904_converte_serie_corretamente():
    conteudo = _xlsx([["Data"], [45000]], epoch=CALENDAR_MAC_1904)
    registros = _ler(conteudo, {"Data": "date"}, {"Data": {"tipo": "data"}})
    assert registros[0]["Data"] == "2027-03-16"


def test_b5_planilha_1900_continua_igual():
    conteudo = _xlsx([["Data"], [45000]])
    registros = _ler(conteudo, {"Data": "date"}, {"Data": {"tipo": "data"}})
    assert registros[0]["Data"] == "2023-03-15"


def test_b6_numero_em_coluna_texto_aceito_por_padrao():
    conteudo = _xlsx([["CEP"], [1234]])
    assert _ler(conteudo, {"CEP": "string"})[0]["CEP"] == "1234"


def test_b6_numero_em_coluna_texto_rejeitado_quando_ligado():
    conteudo = _xlsx([["CEP"], [1234]])
    with pytest.raises(ExcelQualidadeError, match="linha 2, CEP"):
        _ler(conteudo, {"CEP": "string"}, limites={"rejeitar_numero_em_texto": True})


def test_b6_texto_e_formato_id_continuam_aceitos_quando_ligado():
    conteudo = _xlsx([["CEP", "Cod"], ["01234", 7]])
    registros = _ler(
        conteudo, {"CEP": "string", "Cod": "string"},
        formatos={"Cod": {"tipo": "id"}}, limites={"rejeitar_numero_em_texto": True},
    )
    assert registros[0]["CEP"] == "01234"
    assert registros[0]["Cod"] == "7"


def test_rejeitar_numero_em_texto_deve_ser_booleano():
    arquivo = {"drive_id": "d", "item_id": "i", "aba": "Dados"}
    with pytest.raises(ExcelQualidadeError):
        _excel_config_leitura([arquivo], {"A": "string"}, [], {"rejeitar_numero_em_texto": 1})


# --- Comportamentos que não podem mudar --------------------------------------

def test_float_e_normalizado_para_15_digitos():
    assert _excel_texto(0.1 + 0.2) == "0.3"


def test_data_nativa_vira_iso():
    assert _excel_texto(date(2024, 1, 5)) == "2024-01-05"
