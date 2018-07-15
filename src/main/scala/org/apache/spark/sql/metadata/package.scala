package org.apache.spark.sql

import org.apache.spark.sql.types.StructType

/**
 * Created by KPrajapati on 7/15/2018.
 */
package object metadata {

  implicit class DataFrameDataWrapper[T](val dsWriter: DataFrameWriter[T]) {

    def saveAsTable(tableName: String, schema: StructType)(implicit spark: SparkSession): Unit = {
      dsWriter.saveAsTable(tableName)
      UpdateSparkMetadata.alterTableSchema(tableName, schema)(spark)
    }
  }

}
