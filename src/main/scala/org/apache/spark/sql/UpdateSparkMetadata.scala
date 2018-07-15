package org.apache.spark.sql

import scala.language.implicitConversions

import org.apache.spark.sql.types.StructType

/**
 * Created by KPrajapati on 7/15/2018.
 */
object UpdateSparkMetadata {
  def alterTableSchema(_table: String, schema: StructType)(implicit spark: SparkSession): Unit = {
    spark.sessionState.catalog.alterTableSchema(
      spark.sessionState.sqlParser.parseTableIdentifier(_table), schema
    )
  }
}
