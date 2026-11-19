"""Leitura e validação de planilhas com histórico em Delta."""

from ._core import (
    ExcelQualidadeError,
    conectar_sharepoint,
    ler_excel_validado,
    validar_dataframe,
    registrar_snapshot,
    listar_snapshots,
    ler_snapshot,
    ler_acumulado,
)

__all__ = [
    "ExcelQualidadeError",
    "conectar_sharepoint",
    "ler_excel_validado",
    "validar_dataframe",
    "registrar_snapshot",
    "listar_snapshots",
    "ler_snapshot",
    "ler_acumulado",
]
