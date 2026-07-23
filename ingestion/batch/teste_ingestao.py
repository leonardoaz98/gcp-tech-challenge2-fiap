import basedosdados as bd
import os
from dotenv import load_dotenv

load_dotenv()
PROJECT = os.getenv("GCP_PROJECT_ID")

df = bd.read_sql(
    query="SELECT * FROM `basedosdados.br_inep_avaliacao_alfabetizacao.uf` LIMIT 100",
    billing_project_id=PROJECT
)

print(df.shape)
print(df.dtypes)
df.to_parquet("teste.parquet", index=False)
print("parquet gravado")
