import java.net._
import java.io._
import scala.io._

import java.util.Properties
import java.util.Date
import java.util.Random
import java.util.TimeZone
import javax.xml.bind.DatatypeConverter
import java.text.SimpleDateFormat

import kafka.producer.{ProducerConfig, KeyedMessage, Producer}

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.utils.UUIDs

object Config {
	//set properties for all services used for script
	val sparkMasterHost = "127.0.0.1"
	val cassandraHost = "127.0.0.1"
	val cassandraKeyspace = "loyalty"
	val cassandraCfCouponCounters = "coupon_counters"
	val cassandraCfCouponEvents = "coupon_events"
	val cassandraCfPDs = "personalized_deals"
	val zookeeperHost = "localhost:2181"
	val kafkaHost = "localhost:9092"
	val kafkaTopic = "test"
	val kafkaConsumerGroup = "spark-streaming-test"
}

//define class to hold records as they are parsed from the Kafka queue
case class Record(bucket:Long, time:Date, offer_id:String, count:Long)
//define class to hold aggregates as they wait to be written to Cassandra
case class RecordCount(bucket:Long, offer_id:String, count:Long)

object StreamConsumer {
//setup and process all elements needed to consume messages from kafka and write to C*

	//set up the contexts we will need to process messages
	def setup() : (SparkContext, StreamingContext, CassandraConnector) = {
		val sparkConf = new SparkConf(true)
			.set("spark.cassandra.connection.host",Config.cassandraHost)
			.set("spark.cleaner.ttl","3600")
			.setMaster("local[12]")
			.setAppName(getClass.getSimpleName)

		//connect to the spark cluster
		val sc = new SparkContext(sparkConf)
		val ssc = new StreamingContext(sc,Seconds(1))
		val cc = CassandraConnector(sc.getConf)
		return (sc,ssc,cc)
	}
	//need this to parse date from whatever format it is in Kafka. Important if using different language on consumer side.
	def parseDate(str:String) : Date = {
			val parsed_date = new java.text.SimpleDateFormat("yyyy-mm-dd hh:mm:ss").parse(str)
			return parsed_date
		}
	//given a time, calculates the "minute bucket" used for aggregate and event storage bucketing
	def minuteBucket(d:Date) : Long = {
		return d.getTime() / (60*1000)
	}
	/*take an incoming Kafka message, parse and return a Record object. Note that the return statement
	needs to define elemetns in order of how they return after being parsed from the kafka string.
	*/
	def parseMessage(msg:String) : Record = {
		val arr = msg.split(",")
		val time = parseDate(arr(1))
		return Record(minuteBucket(time), time, arr(0), arr(2).toInt)
	}
	//Create Records objects from all messages. Create aggregate objects using combineByKey
	def process(ssc : StreamingContext, input  : DStream[String]) {
		val parsedRecords = input.map(parseMessage)
		val bucketedRecords = parsedRecords.map(record => ((record.bucket, record.offer_id),record))
		val	bucketedCounts = bucketedRecords.combineByKey(
			(record:Record) => record.count,
			(count:Long, record:Record) => (count + record.count),
			(c1:Long, c2:Long) => (c1 + c2),
			new HashPartitioner(1)
			)

		val flattenCounts = bucketedCounts.map((agg) => RecordCount(agg._1._1,agg._1._2,agg._2))

		parsedRecords.print()
		//insert records into Cassandra
		parsedRecords.saveToCassandra(Config.cassandraKeyspace, Config.cassandraCfCouponEvents)
		flattenCounts.saveToCassandra(Config.cassandraKeyspace, Config.cassandraCfCouponCounters)
		

		sys.ShutdownHookThread {
			ssc.stop(true,true)
		}

		ssc.start()
		ssc.awaitTermination()
	}
}

object KafkaConsumer {
  def main(args: Array[String]) {
  	//set values for contexts and connector to use in the process function
    val (sc, ssc, cc) = StreamConsumer.setup()
    //create the DStream
    val input = KafkaUtils.createStream(
      ssc,
      Config.zookeeperHost,
      Config.kafkaConsumerGroup,
      Map(Config.kafkaTopic -> 1)).map(_._2)
    //run the consumer process
    StreamConsumer.process(ssc, input)
  }
}