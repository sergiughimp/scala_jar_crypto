package spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}
object incrementalCrypto {
  def main(args: Array[String]): Unit = {
    val url = "jdbc:postgresql://ec2-13-40-49-105.eu-west-2.compute.amazonaws.com:5432/testdb"
    val properties = new java.util.Properties()
    properties.setProperty("user", "consultants")
    properties.setProperty("password", "WelcomeItc@2022")
    properties.put("driver", "org.postgresql.Driver")

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val spark = SparkSession.builder().appName("JDBCExample").getOrCreate()
    // *****************************************************************************************************
    // Ethereum Incremental
    // *****************************************************************************************************

    val max_df_ethereum = spark.sql("select max(ethereum_id) from scalagroup.ethereum_initialdataframe").first()
    val cdc_df_ethereum = max_df_ethereum.get(0)
    val query_df_ethereum = s"(select * from ethereum where cast(ethereum_id as int) > $cdc_df_ethereum) as tb1_ethereum"

    val df_ethereum = spark.read.jdbc(url, query_df_ethereum, properties)
    df_ethereum.show(false)
    df_ethereum.write.mode(SaveMode.Append).saveAsTable("scalagroup.ethereum_initialdataframe")
  }
}
