import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{ArrayType, DateType, IntegerType, LongType, StringType, StructType, TimestampType}

object StreamTwitterData {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._
    val lines = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "twitter-data1")
      .option("startingOffsets", "latest")
      .load()

    val df = lines.selectExpr("CAST(value AS STRING)")

    val schema = new StructType()
      .add("timeStamp", LongType)
      .add("hashTags", ArrayType(StringType))

    val df1 = df.select(from_json(col("value"), schema).as("twitterData")).select("twitterData.*")

    val df2 = df1.withColumn("timeStampInSeconds", col("timeStamp")/1000)

    val df3 = df2.select(
      col("timeStampInSeconds").cast(TimestampType).as("timeStampInSeconds"),
      col("hashTags")
    )
    val df4 = df3.select(col("timeStampInSeconds"), explode(col("hashTags")).as("hashTagsArray"))

    val wordCounts = df4
      .withWatermark("timeStampInSeconds", "10 minutes")
      .groupBy(
      window(col("timeStampInSeconds"), "10 minutes", "5 minutes"),
      col("hashTagsArray")
    ).count().orderBy(desc("count"))

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("5 minutes"))
      .start()

    /*al query = wordCounts
      .select(to_json(struct($"*")).cast(StringType).alias("value"))
      .writeStream
      .queryName("abc")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "twitter-data-output")
      .option("checkpointLocation", "/tmp/abc")
      .outputMode("append")
      .start()*/

      /*val query = wordCounts
        .select(to_json(struct($"*")).cast(StringType).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("checkpointLocation", "/tmp/abc")
        .option("topic", "twitter-data-output")
        .start()*/

    query.awaitTermination()

  }
}
