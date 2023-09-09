import pandas as pd
from pyspark.sql import SparkSession
import pandas as pd
import sys

def spark_con():
    try:
        spark = (SparkSession.builder
                 .appName("SAP_to_Hive")
                 .config("spark.master", "yarn")
                 .config("spark.driver.memory", "32g")
                 .config("spark.executor.memory", "8g")
                 .config("spark.executor.instances", "36")
                 .config("spark.executor.cores", "4")
                 .config("spark.jars", "/root/ngdbc-2.17.12.jar")
                 .config("hive.metastore.uris", "thrift://hdp01-preprod.geepas.local:9083")
                 .config("hive.metastore.client.capability.check", "false")
                 .enableHiveSupport()
                 .getOrCreate())

        spark.conf.set("spark.sql.shuffle.partitions", 200)
        spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
        return spark
    except Exception as e:
        print(f"Error while creating Spark Session: {e}")
        return None

def main(sap_server, schema, table_name, hivedb, spark):
    try:
        url = f"jdbc:sap://{sap_server}/"
        full_table_name = f'"{schema}"."{table_name}"'  # Enclosing table name in quotes
        print(f"Trying to read table: {full_table_name}")

        df = (spark.read.format("jdbc")
              .option("driver", "com.sap.db.jdbc.Driver")
              .option("url", url)
              .option("user", "BIG_DATA_USER")
              .option("password", "Geepas123")
              .option("dbtable", full_table_name)
              .option("fetchSize", "10000")
              .option("numPartitions", 200)
              .load())

        if df.rdd.isEmpty():
            print("Data frame is empty. Exiting.")
            return

        df = df.repartition(200)  # Repartition for performance
        table_namef = table_name.replace("\"", "").replace("/", "_")

        try:
            df.write.format("orc").option("path", "/demo").mode("overwrite").saveAsTable(f"{hivedb}.{table_namef}")
        except Exception as e:
            print(f"Error while writing to Hive: {e}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    df = pd.read_csv('test.csv')
    row_count = len(df)
    current_row = 0

    spark_session = spark_con()
    if spark_session is None:
        print("Exiting due to Spark session creation failure.")
        sys.exit(1)

    while current_row < row_count:
        row = df.iloc[current_row]
        sap_server = row['host']
        schema = row['db']
        table_name = row['table']
        hivedb = row['destination']
# Adjust this to match your CSV file column name for Hive table names

        main(sap_server, schema, table_name, hivedb, spark_session)

        current_row += 1
