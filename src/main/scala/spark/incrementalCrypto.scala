package spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SaveMode, SparkSession}
object incrementalCrypto {
  def main(args: Array[String]): Unit = {
    val url = "jdbc:postgresql://ec2-13-40-49-105.eu-west-2.compute.amazonaws.com:5432/testdb"
    val properties = new java.util.Properties()
    properties.setProperty("user", "consultants")
    properties.setProperty("password", "WelcomeItc@2022")
    properties.put("driver", "org.postgresql.Driver")

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

    // *****************************************************************************************************

    val max_filtered_df_ethereum = spark.sql("select max(ethereum_id) from scalagroup.ethereum_filteredbyprice").first()
    val cdc_filtered_df_ethereum = max_filtered_df_ethereum.get(0)
    val query_filtered_df_ethereum = s"(select * from ethereum where cast(ethereum_id as int) > $cdc_filtered_df_ethereum) as tb2_ethereum"

    val filtered_df_ethereum = spark.read.jdbc(url, query_filtered_df_ethereum, properties)
    filtered_df_ethereum.show(false)
    filtered_df_ethereum.write.mode(SaveMode.Append).saveAsTable("scalagroup.ethereum_filteredbyprice")

    // *****************************************************************************************************
    val max_sorted_df_ethereum = spark.sql("select max(ethereum_id) from scalagroup.ethereum_sortedbykeybyprice").first()
    val cdc_sorted_df_ethereum = max_sorted_df_ethereum.get(0)
    val query_sorted_df_ethereum = s"(select * from ethereum where cast(ethereum_id as int) > $cdc_sorted_df_ethereum) as tb3_ethereum"

    val sorted_df_ethereum = spark.read.jdbc(url, query_sorted_df_ethereum, properties)
    sorted_df_ethereum.show(false)
    sorted_df_ethereum.write.mode(SaveMode.Append).saveAsTable("scalagroup.ethereum_sortedbykeybyprice")

    // *****************************************************************************************************
    val max_mean_price_df_ethereum = spark.sql("select max(ethereum_id) from scalagroup.ethereum_mean_price").first()
    val cdc_mean_price_df_ethereum = max_mean_price_df_ethereum.get(0)
    val query_mean_price_df_ethereum = s"(select * from ethereum where cast(ethereum_id as int) > $cdc_mean_price_df_ethereum) as tb4_ethereum"
    val mean_price_df_ethereum = spark.read.jdbc(url, query_mean_price_df_ethereum, properties)
    val drop_mean_price_df_ethereum_null_column = mean_price_df_ethereum.drop("ethereum_NULL")
    // Compute the ethereum_mean_price of the "ethereum_price" column
    val ethereum_mean_price = mean_price_df_ethereum.select(mean(col("ethereum_price"))).first().getDouble(0)
    val mean_price_df_ethereum_no_null = drop_mean_price_df_ethereum_null_column.withColumn("ethereum_mean_price", lit(ethereum_mean_price))
    mean_price_df_ethereum_no_null.show(false)
    mean_price_df_ethereum_no_null.write.mode(SaveMode.Append).saveAsTable("scalagroup.ethereum_mean_price")
  }
}
