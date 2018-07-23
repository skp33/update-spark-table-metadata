# updating-spark-table-metadata
Spark doesn't support metadata update in append mode for a table. With this utility one can update the table metadata in append mode.

## How to use:
Import the utility jar in your project and import the below package:

```
import org.apache.spark.sql.metadata._
```
Then when you've to write the table, call the 'saveAsTable' method as usual and this time pass an additional argument for table schema like below. This additional argument will be the schema with metadata that you want to update.

```scala
aggOrder.write.mode("append").saveAsTable("daily_order_count", aggOrder.schema)
```
