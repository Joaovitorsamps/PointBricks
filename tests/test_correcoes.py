"""Correções de conversões que alteravam o valor sem aviso."""

from io import BytesIO

import pytest
from openpyxl import Workbook
from openpyxl.utils.datetime import CALENDAR_MAC_1904

from PointBricks._core import (
    ExcelQualidadeError,
    _excel_compilar_formatos,
    _excel_config_leitura,
    _excel_ler,
    _fmt_data,
    _fmt_numero,
)


def regra(tipo_schema, **formato):
    return _excel_compilar_formatos({"C": formato}, {"C": tipo_schema})["C"]


# --- Símbolo monetário no meio do número --------------------------------------

@pytest.mark.parametrize("texto", ["10R$5", "1R$.234,56", "-10R$,00"])
def test_simbolo_no_meio_do_numero_e_rejeitado(texto):
    with pytest.raises(ExcelQualidadeError, match="Símbolo monetário"):
        _fmt_numero(texto, regra("decimal(18,2)", tipo="moeda"))


@pytest.mark.parametrize("texto, esperado", [
    ("10,50 R$", "10.5"),
    ("10,50R$", "10.5"),
    ("-10,00 R$", "-10"),
    ("+ R$ 10,00", "10"),
])
def test_simbolo_no_fim_continua_aceito(texto, esperado):
    assert _fmt_numero(texto, regra("decimal(18,2)", tipo="moeda")) == esperado


def test_simbolo_personalizado():
    r = regra("decimal(18,2)", tipo="moeda", simbolo="US$",
              separador_decimal=".", separador_milhar=",")
    assert _fmt_numero("US$ 1,234.56", r) == "1234.56"
    with pytest.raises(ExcelQualidadeError):
        _fmt_numero("1US$234.56", r)


# --- Data base 1904 -----------------------------------------------------------


def xlsx(linhas, epoch=None):
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


def ler_data(conteudo, tipo="data"):
    arquivo = {"drive_id": "d", "item_id": "i", "aba": "Dados"}
    cfg = _excel_config_leitura([arquivo], {"Data": "date"}, [], None, {"Data": {"tipo": tipo}})
    return _excel_ler(conteudo, arquivo, cfg, "Fonte 1", 1000)[0]["Data"]


def test_planilha_1904_converte_serie_corretamente():
    assert ler_data(xlsx([["Data"], [45000]], epoch=CALENDAR_MAC_1904)) == "2027-03-16"


def test_planilha_1904_periodo():
    assert ler_data(xlsx([["Data"], [45000]], epoch=CALENDAR_MAC_1904), "periodo") == "2027-03-01"


def test_planilha_1900_continua_igual():
    assert ler_data(xlsx([["Data"], [45000]])) == "2023-03-15"


def test_fmt_data_sem_epoch_usa_1900():
    assert _fmt_data(45000, regra("date", tipo="data")) == "2023-03-15"
