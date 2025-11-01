
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col


spark = SparkSession.builder.appName("ingestao-bronze").getOrCreate()

input_path = "/Volumes/workspace/portfolio_spark/datalake/vendas.csv" 

bronze_path = "/Volumes/workspace/portfolio_spark/datalake/delta/bronze/vendas" 

# pra  não esquecer os caminhos
# silver_path = "/Volumes/workspace/portfolio_spark/datalake/delta/silver/vendas" 
# gold_path = "/Volumes/workspace/portfolio_spark/datalake/delta/gold/revenue_daily"

# lendo o csv pelo volume
print(f"Tentando ler o arquivo CSV em: {input_path}")
try:
    
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
    print(f"Sucesso ao ler o arquivo CSV. O DataFrame tem {df.count()} linhas.")
except Exception as e:
    
    print(f"ERRO CRÍTICO: Não foi possível ler o arquivo em {input_path}.")
    print(f"Verifique o Volume e o nome do arquivo.")
    raise e


print("Esquema e Amostra do DataFrame (Camada Bronze):")
df.printSchema()
df.show(5, False)

#  só alinhando as datas

df_bronze = df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))


# escrevendo na camada bronze
print(f"Escrevendo o DataFrame transformado na Camada Bronze em: {bronze_path}")
# usar isso aqui pra reescrever a tabela, nao esquecer
df_bronze.write.format("delta").mode("overwrite").save(bronze_path)

print("-" * 50)
print("ETL da Camada Bronze concluído. O arquivo CSV foi transformado em uma Tabela Delta em:")
print(bronze_path)

