from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.errors import NumberFormatException, ArithmeticException, ArrayIndexOutOfBoundsException

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
    try:
        # O castear un campo string a int
        df.select(df.name.cast("int")).collect()
    except NumberFormatException as e:
        print("Error when casting string to integer:\n", e)

    # Excepcion cuando se intenta dividir por cero
    try:
        df.select((df.age / 0).alias("age_div_zero")).collect()
    except ArithmeticException as e:
        print("Error when dividing by zero:\n", e)

    # Excepcion al intentar acceder a un elemento no existente en una lista
    try:
        df = spark.createDataFrame([([10, 20, 30],)], ["my_array"])
        df.select(col("my_array").getItem(5)).show()
    except ArrayIndexOutOfBoundsException as e:
        print("Error when accessing out of bounds index:\n", e)


if __name__ == "__main__":
    main()
    spark.stop()
