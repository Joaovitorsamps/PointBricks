"""Correções de conversões que alteravam o valor sem aviso."""

from io import BytesIO

import pytest
from openpyxl import Workbook
from openpyxl.utils.datetime import CALENDAR_MAC_1904

from PointBricks._core import (
    ExcelQualidadeError,
    _excel_compilar_formatos,
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
