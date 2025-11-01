# camada silver
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("refinamento-silver").getOrCreate()

bronze_path = "/Volumes/workspace/portfolio_spark/datalake/delta/bronze/vendas" 
silver_path = "/Volumes/workspace/portfolio_spark/datalake/delta/silver/vendas" 

print(f"Tentando ler a Camada Bronze em: {bronze_path}")
try:

    df_bronze = spark.read.format("delta").load(bronze_path)
    print("Sucesso ao ler a Camada Bronze.")
except Exception as e:
    print(f"ERRO: Não foi possível ler a Camada Bronze. Certifique-se de que o script 01 foi executado.")
    raise e

print("Esquema da Bronze:")
df_bronze.printSchema()
df_bronze.show(5, False)

df_silver = df_bronze.withColumn(
    "total_revenue", 
    col("price") * col("quantity")
).select(
    "order_id",
    "customer_id",
    "order_date",
    "product_id",
    "price", 
    "quantity",
    "total_revenue" 
)

print(f"Escrevendo o DataFrame refinado na Camada Silver em: {silver_path}")
df_silver.write.format("delta").mode("overwrite").save(silver_path)

print("-" * 50)
print("ETL da Camada Silver concluído. Receita Total calculada e salva em:")
print(silver_path)
