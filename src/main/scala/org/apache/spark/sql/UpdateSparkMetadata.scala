package org.apache.spark.sql

import java.util.Locale

import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Created by KPrajapati on 7/15/2018.
 */
object UpdateSparkMetadata {
/*
  def alterTableSchema(sourceTable: String, schema: StructType)(implicit spark: SparkSession): Unit = {
    spark.sessionState.catalog.alterTableSchema(
      spark.sessionState.sqlParser.parseTableIdentifier(sourceTable), schema
    )
  }
*/

  def alterTableSchema(
      sourceTable: String,
      newSchema: StructType)(implicit spark: SparkSession): Unit = {
    val catalog = spark.sessionState.catalog
    val conf = spark.sqlContext.conf
    val currentDb = catalog.getCurrentDatabase
    val externalCatalog = catalog.externalCatalog
    val identifier = spark.sessionState.sqlParser.parseTableIdentifier(sourceTable)

    def formatDatabaseName(name: String): String = {
      if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
    }

    def formatTableName(name: String): String = {
      if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
    }

    def requireTableExists(name: TableIdentifier): Unit = {
      if (!tableExists(name)) {
        val db = name.database.getOrElse(currentDb)
        throw new NoSuchTableException(db = db, table = name.table)
      }
    }

    def tableExists(name: TableIdentifier): Boolean = synchronized {
      val db = formatDatabaseName(name.database.getOrElse(currentDb))
      val table = formatTableName(name.table)
      externalCatalog.tableExists(db, table)
    }

    def requireDbExists(db: String): Unit = {
      if (!databaseExists(db)) {
        throw new NoSuchDatabaseException(db)
      }
    }

    def databaseExists(db: String): Boolean = {
      val dbName = formatDatabaseName(db)
      externalCatalog.databaseExists(dbName)
    }

    def checkDuplication(fields: Seq[StructField]): Unit = {
      val columnNames = if (conf.caseSensitiveAnalysis) {
        fields.map(_.name)
      } else {
        fields.map(_.name.toLowerCase)
      }
      if (columnNames.distinct.length != columnNames.length) {
        val duplicateColumns = columnNames.groupBy(identity).collect {
          case (x, ys) if ys.length > 1 => x
        }
        throw new AnalysisException(s"Found duplicate column(s): ${duplicateColumns.mkString(", ")}")
      }
    }

    def columnNameResolved(schema: StructType, colName: String): Boolean = {
      schema.fields.map(_.name).exists(conf.resolver(_, colName))
    }

    val db = formatDatabaseName(identifier.database.getOrElse(currentDb))
    val table = formatTableName(identifier.table)
    val tableIdentifier = TableIdentifier(table, Some(db))
    requireDbExists(db)
    requireTableExists(tableIdentifier)
    checkDuplication(newSchema)

    val catalogTable = externalCatalog.getTable(db, table)
    val oldSchema = catalogTable.schema

    // not supporting dropping columns yet
    val nonExistentColumnNames = oldSchema.map(_.name).filterNot(columnNameResolved(newSchema, _))
    if (nonExistentColumnNames.nonEmpty) {
      throw new AnalysisException(
        s"""
           |Some existing schema fields (${nonExistentColumnNames.mkString("[", ",", "]")}) are
           |not present in the new schema. We don't support dropping columns yet.
         """.stripMargin)
    }

    // assuming the newSchema has all partition columns at the end as required
    externalCatalog.alterTableSchema(db, table, newSchema)
  }
}
