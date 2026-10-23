from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, avg, max, desc, round as spark_round

def main():
    print("==========================================================")
    print(">>> 1. INICIALIZANDO SESION DE APACHE SPARK (BATCH) <<<")
    print("==========================================================")
    spark = SparkSession.builder \
        .appName("Analisis_Batch_Fraude_Financiero") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    hdfs_path = "hdfs://localhost:9000/Tarea3/creditcard.csv"

    print("\n>>> 2. CARGANDO DATASET DESDE HDFS...")
    df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

    print("\n>>> 3. ESQUEMA DEL DATAFRAME:")
    df.printSchema()

    print("\n>>> 4. PRIMEROS 5 REGISTROS DEL DATASET:")
    df.select("Time", "V1", "V2", "Amount", "Class").show(5)

    print("\n>>> 5. VALIDACION DE VALORES NULOS O VACIOS:")
    df.select([count(when(col(c).isNull(), c)).alias(c) for c in ["Time", "Amount", "Class"]]).show()

    total_registros = df.count()
    print(f"\n>>> TOTAL DE TRANSACCIONES ANALIZADAS: {total_registros:,}")

    print("\n==========================================================")
    print(">>> 6. ANALISIS DE DESBALANCE: TRANSACCIONES LEGITIMAS VS FRAUDE <<<")
    print("==========================================================")
    df.groupBy("Class").agg(
        count("Class").alias("Total_Transacciones"),
        spark_round((count("Class") / total_registros) * 100, 4).alias("Porcentaje_%")
    ).show()

    print("\n==========================================================")
    print(">>> 7. ANALISIS ESTADISTICO EXPLORATORIO (EDA) DEL MONTO (AMOUNT) <<<")
    print("==========================================================")
    df.select("Amount").summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show()

    print("\n==========================================================")
    print(">>> 8. MONTO PROMEDIO Y MAXIMO: LEGITIMAS (0) VS FRAUDES (1) <<<")
    print("==========================================================")
    df.groupBy("Class").agg(
        count("Amount").alias("Cantidad_Operaciones"),
        spark_round(avg("Amount"), 2).alias("Monto_Promedio_USD"),
        spark_round(max("Amount"), 2).alias("Monto_Maximo_USD")
    ).show()

    print("\n==========================================================")
    print(">>> 9. DETECCION DE FRAUDES DE ALTO IMPACTO (AMOUNT > 1000) <<<")
    print("==========================================================")
    fraudes_altos = df.filter((col("Class") == 1) & (col("Amount") > 1000)) \
                      .select("Time", "Amount", "Class", "V1", "V2", "V3") \
                      .orderBy(desc("Amount"))
    
    print(f"Total de fraudes criticos con monto > 1000 USD: {fraudes_altos.count()}")
    fraudes_altos.show(10)

    print("\n>>> ANALISIS EN BATCH FINALIZADO CON EXITO EN APACHE SPARK <<<")
    spark.stop()

if __name__ == "__main__":
    main()
