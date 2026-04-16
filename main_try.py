from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, try_divide
from pyspark.sql.types import IntegerType

"""
Archivo que usa la forma try_cast como alternativa a las excepciones 
para manejar errores en Spark 4.
"""

# Configura Spark para habilitar el modo ANSI
spark = SparkSession.builder \
    .config("spark.sql.ansi.enabled", "true") \
    .appName("Spark ANSI Example") \
    .getOrCreate()


def main():

    # Crea un dataframe con datos aleatorios
    data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["name", "age"])

    # Excepcion al intentar sumar un número a una cadena
    df.select("*", df.name.try_cast(IntegerType()).alias("name_as_int")).show()

    # Excepcion cuando se intenta dividir por cero
    df.select("*", try_divide(df.age, lit(0)).alias("age_div_zero")).show()


if __name__ == "__main__":
    main()
    spark.stop()
