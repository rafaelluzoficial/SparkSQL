# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark.sql("CREATE DATABASE DESPACHANTE")
spark.sql("SHOW DATABASES").show()

# apontando para o BD no qual se deseja trabalhar
spark.sql("USE DESPACHANTE").show()

# COMMAND ----------

# Criando um DF e carregando em uma tabela do BD
arqSchema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
path_dataset = '/FileStore/tables/despachantes.csv'

# Lê arquivos formato CSV
df_despachantes1 = spark.read.csv(path_dataset, header=False, schema=arqSchema)
df_despachantes1.show()

# Carregando dados do DF na tabela do BD
df_despachantes1.write.saveAsTable("Despachantes")

# COMMAND ----------

# Carregando dados do DF na tabela do BD no modo sobrescrever tabela
df_despachantes1.write.mode("overwrite").saveAsTable("Despachantes")

# Carregando dados do DF na tabela do BD no modo adicionar novos dados
# df_despachantes1.write.mode("append").saveAsTable("Despachantes")

spark.sql("SHOW TABLES").show()
spark.sql("SELECT * FROM Despachantes").show()

# COMMAND ----------

# Criando DF com consultas SQL
df_despachantes2 = spark.sql("SELECT * FROM Despachantes")
df_despachantes2.show()

# COMMAND ----------

# Criando uma tabela não gerenciada (externa ao BD)
df_despachantes2.write.option("path", "/FileStore/Despachante").saveAsTable("Despachantes_ng")
spark.sql("Despachantes_ng").show()

# COMMAND ----------

# Verificando se a tabela é gerenciada ou não

# Se no resultado aparecer o campo LOCATION com o path, não é gerenciada
spark.sql("SHOW CREATE TABLE Despachantes").show(truncate=False)
spark.sql("SHOW CREATE TABLE Despachantes_ng").show(truncate=False)

# A indicação de gerenciada está no campo tableType=MANAGED ou EXTERNAL
spark.catalog.listTables()

# COMMAND ----------

# Criando Views

# View temporária
# df_despachantes2.createOrReplaceTempView("vw_despachantes2")
spark.sql("SELECT * FROM vw_despachantes2").show()

# View Global
# df_despachantes2.createOrReplaceGlobalTempView("vw_g_despachantes2")
spark.sql("SELECT * FROM global_temp.vw_g_despachantes2").show()

# Criação via SparkSQL
# spark.sql("CREATE OR REPLACE TEMP VIEW vw_despachantes3 AS SELECT * FROM Despachantes")
spark.sql("SELECT * FROM vw_despachantes3").show()

# COMMAND ----------

# Trabalhando com JOINS
# Joins entre tabelas Despachantes e Reclamacoes

# rec_Schema = "idrec INT, datarec STRING, iddesp INT"
# reclamacoes = spark.read.csv("/FileStore/tables/reclamacoes.csv", header=False, schema=rec_Schema)

# reclamacoes.write.saveAsTable("Reclamacoes")
spark.sql("SELECT * FROM Reclamacoes").show()
spark.sql("SELECT * FROM Despachantes").show()

df_despachantes1.join(reclamacoes, df_despachantes1.id == reclamacoes.iddesp, "inner").select("idrec", "datarec", "iddesp", "nome").show()
spark.sql("SELect Reclamacoes.*, Despachantes.nome FROM Despachantes INNER JOIN Reclamacoes ON (Despachantes.id = Reclamacoes.iddesp) ORDER BY Reclamacoes.iddesp").show()

df_despachantes1.join(reclamacoes, df_despachantes1.id == reclamacoes.iddesp, "left").select("idrec", "datarec", "iddesp", "nome").show()
spark.sql("SELect Reclamacoes.*, Despachantes.nome FROM Despachantes LEFT JOIN Reclamacoes ON (Despachantes.id = Reclamacoes.iddesp) ORDER BY Reclamacoes.iddesp").show()
