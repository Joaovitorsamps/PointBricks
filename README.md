# PointBricks

Instale a distribuição com `pip install PointBricks` após sua publicação. Para instalar a partir deste repositório, use `pip install .`.

```python
import PointBricks
from azure.identity import ClientSecretCredential

# O ambiente protegido da organização fornece estas três variáveis.
credencial = ClientSecretCredential(tenant_id, client_id, segredo_app)
login = PointBricks.conectar_sharepoint(credencial)
df = PointBricks.ler_excel_validado(login=login, arquivos=arquivos, schema=schema)
```

Defina a lista de arquivos e o schema conforme os exemplos e contratos do [notebook](Point_Bricks.ipynb). As operações de DataFrame usam uma sessão PySpark ativa. Para snapshots, use um runtime Databricks com Delta Lake, VARIANT, variant_get e to_variant_object disponíveis. O runtime fornece PySpark e Delta.

```python
PointBricks.registrar_snapshot(
    df, tabela="catalogo.schema.historico", id_fonte="precos_lojas", schema=schema
)
```
