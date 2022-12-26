import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.cassandra._

import com.datastax.oss.driver.api.core.uuid.Uuids // com.datastax.cassandra:cassandra-driver-core:4.0.0
import com.datastax.spark.connector._              // com.datastax.spark:spark-cassandra-connector_2.11:2.4.3

case class BigBasket(id: Int,product: String,category: String,sub_category: String,brand: String,sale_price: Double,market_price: Double,type_ : String,rating: Double,description: String)

// initialize Spark
val spark = SparkSession
	.builder
	.appName("Stream Handler")
	.config("spark.cassandra.connection.host", "localhost")
	.config("spark.cassandra.auth.username", "cassandra")
    .config("spark.cassandra.auth.password", "cassandra")
	.getOrCreate()

import spark.implicits._

// read from Kafka
val inputDF = spark
	.readStream
	.format("kafka") // org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5
	.option("kafka.bootstrap.servers", "localhost:9092")
	.option("subscribe", "nhom3topic")
	.load()

// inputDF.show(false)

val rawDF = inputDF.selectExpr("CAST(value AS STRING)").as[String]

// split each row on comma, load it to the case class
val expandedDF = rawDF.map(row => row.split("##"))
	.map(row => BigBasket(
		row(0).toInt,
		row(1).toString,
		row(2).toString,
		row(3).toString,
		row(4).toString,
		row(5).toDouble,
		row(6).toDouble,
		row(7).toString,
		row(8).toDouble,
		row(9).toString
	))

// create a dataset function that creates UUIDs
val makeUUID = udf(() => Uuids.timeBased().toString)


val summaryWithIDs = expandedDF.withColumn("uuid", makeUUID())

// write dataframe to Cassandra
val query = summaryWithIDs
	.writeStream
	.trigger(Trigger.ProcessingTime("1 seconds"))
	.foreachBatch { (batchDF: DataFrame, batchID: Long) =>
		println(s"Writing to Cassandra batchID $batchID")
		// batchDF.show(false)
		batchDF.printSchema()
		batchDF.write
			.cassandraFormat("bigbasket", "nhom3clouds") // table, keyspace
			.mode("append")
			.save()
	}
	.outputMode("update")
	.start()

// until ^C
query.awaitTermination()
