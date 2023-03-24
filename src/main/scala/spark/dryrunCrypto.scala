package spark

import org.apache.spark.sql.{SaveMode, SparkSession}
object dryrunCrypto {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val url = "jdbc:postgresql://ec2-13-40-49-105.eu-west-2.compute.amazonaws.com:5432/testdb"
    val properties = new java.util.Properties()
    properties.setProperty("user", "consultants")
    properties.setProperty("password", "WelcomeItc@2022")
    properties.put("driver", "org.postgresql.Driver")

    import spark.implicits._

    // *****************************************************************************************************
    // Ethereum table Transformations
    println("Ethereum Initial DataFrame")
    val df_ethereum = spark.read.jdbc(url, "ethereum", properties)
    df_ethereum.show(false)
    // Create Hive Internal table
    df_ethereum.write.mode(SaveMode.Overwrite).saveAsTable("scalagroup.Ethereum_InitialDataFrame")

    println("Ethereum DataFrame filtered by 'ethereum_price > 1.3'")
    // filter() Transformation = filter the records in an RDD. filtering ethereum_price > "1.3".
    val filtered_df_ethereum = df_ethereum.filter($"ethereum_price" > "1.3")
    filtered_df_ethereum.show(false)
    // Create Hive Internal table
    filtered_df_ethereum.write.mode(SaveMode.Overwrite).saveAsTable("scalagroup.Ethereum_FilteredByPrice")
  }
}
