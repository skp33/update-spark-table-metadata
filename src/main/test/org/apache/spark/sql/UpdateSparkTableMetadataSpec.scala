package org.apache.spark.sql

import org.apache.spark.sql.types.{Metadata, MetadataBuilder}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

/**
 * Created by KPrajapati on 7/15/2018.
 */
class UpdateSparkTableMetadataSpec extends FunSuite with BeforeAndAfterAll {
  implicit var sparkSession : SparkSession = _
  import org.apache.spark.sql.functions._

  val data = Seq((12, 20180411), (5, 20180411), (11, 20180411), (2, 20180412), (29, 20180413),
    (31, 20180414), (18, 20180415), (2, 20180412), (31, 20180413), (8, 20180416), (29, 20180413),
    (31, 20180414), (8, 20180415), (2, 20180412), (23, 20180413), (51, 20180414), (15, 20180415))

  override def beforeAll() {
    sparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .appName("Update metadata of spark table")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
  }

  test("metadata is propagated correctly"){
    val spark = sparkSession
    import spark.implicits._

    val orders = data.toDF("order id", "date")
    val maxDate = orders.agg(max("date")).as[Int].take(1)(0)

    val metadata: Metadata = new MetadataBuilder().putLong("max_dt", maxDate).build

    orders.groupBy('date as("date", metadata)).agg(count("order id") as "order_count")
      .write.saveAsTable("daily_order_count")

    val tillProcessDate = sparkSession.read.table("daily_order_count")
      .schema("date").metadata.getLong("max_dt")

    assert(tillProcessDate == maxDate)
  }

  test("metadata is propagated correctly with overwrite mode"){
    val spark = sparkSession
    import spark.implicits._

    val moreOrders = (data ++ Seq((2, 20180417), (41, 20180417), (25, 20180417),
      (41, 20180418), (25, 20180418))).toDF("order id", "date")

    val maxDate = moreOrders.agg(max("date")).as[Int].take(1)(0)

    val metadata: Metadata = new MetadataBuilder().putLong("max_dt", maxDate).build

    moreOrders.groupBy('date as("date", metadata)).agg(count("order id") as "order_count")
      .write.mode("overwrite").saveAsTable("daily_order_count")

    val tillProcessDate = spark.read.table("daily_order_count")
      .schema("date").metadata.getLong("max_dt")

    assert(tillProcessDate == maxDate)
  }

  test("metadata is not propagated correctly with append mode"){
    val spark = sparkSession
    import spark.implicits._

    val moreOrders = (data ++ Seq((2, 20180417), (41, 20180417), (25, 20180417),
      (41, 20180419), (25, 20180419))).toDF("order id", "date")

    val maxDate = moreOrders.agg(max("date")).as[Int].take(1)(0)

    val metadata: Metadata = new MetadataBuilder().putLong("max_dt", maxDate).build

    moreOrders.groupBy('date as("date", metadata)).agg(count("order id") as "order_count")
      .write.mode("append").saveAsTable("daily_order_count")

    val tillProcessDate = spark.read.table("daily_order_count")
      .schema("date").metadata.getLong("max_dt")

    assert(tillProcessDate != maxDate)
  }

  test("metadata is propagated correctly with append mode"){
    val spark = sparkSession
    import spark.implicits._

    val moreOrders = (data ++ Seq((2, 20180417), (41, 20180417), (25, 20180417),
      (41, 20180420), (25, 20180420))).toDF("order id", "date")

    val maxDate = moreOrders.agg(max("date")).as[Int].take(1)(0)

    val metadata: Metadata = new MetadataBuilder().putLong("max_dt", maxDate).build

    import org.apache.spark.sql.metadata._
    val aggOrder = moreOrders.groupBy('date as("date", metadata)).agg(count("order id") as "order_count")
    aggOrder.write.mode("append").saveAsTable("daily_order_count", aggOrder.schema)

    val tillProcessDate = spark.read.table("daily_order_count")
      .schema("date").metadata.getLong("max_dt")

    assert(tillProcessDate == maxDate)
  }

  override def afterAll() {
    sparkSession.sql("drop table default.daily_order_count")
    sparkSession.stop()
  }
}
