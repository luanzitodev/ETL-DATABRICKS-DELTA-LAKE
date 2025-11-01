
# criação da camada gold para os dados prontoss pro bi

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum


spark = SparkSession.builder.appName("agregacao-gold").getOrCreate()




silver_path = "/Volumes/workspace/portfolio_spark/datalake/delta/silver/vendas" 


gold_path = "/Volumes/workspace/portfolio_spark/datalake/delta/gold/revenue_daily"


print(f"Tentando ler a Camada Silver em: {silver_path}")
try:

    df_silver = spark.read.format("delta").load(silver_path)
    print("Sucesso ao ler a Camada Silver.")
except Exception as e:
    print(f"ERRO: Não foi possível ler a Camada Silver. Certifique-se de que o script 02 foi executado.")
    raise e

# pra dar uma olhada no esquema
print("Esquema da Silver:")
df_silver.printSchema()
df_silver.show(5, False)

# fazendo a agregação e salvando na camada gold

df_gold = df_silver.groupBy("order_date").agg(
    spark_sum("total_revenue").alias("total_revenue_daily")
).sort(
    col("order_date").asc() # ordenar por data pra facilitar a visualização
)

# escrever a camada gold
print(f"Escrevendo o DataFrame agregado na Camada Gold em: {gold_path}")

df_gold.write.format("delta").mode("overwrite").save(gold_path)

# verificar o conteúdo final 
print("\nConteúdo da Tabela Gold 'revenue_daily':")
df_gold.show(5, False)

print("-" * 50)
print("ETL da Camada Gold concluído. Dados agregados por dia salvos em:")
print(gold_path)
