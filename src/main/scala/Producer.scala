import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession

import java.time.ZoneOffset

object Producer extends App {

  import java.util.Properties
  import java.time.LocalDateTime
  import java.time.format.DateTimeFormatter._

  // every second will be factored in this number
  val timeFactor = 0.001
  // topic
  val topic = "text_topic"

  val sparkProps = new Properties()
  sparkProps.put("bootstrap.servers", "localhost:9092")
  sparkProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  sparkProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val spark = SparkSession.builder.master("local[1]").appName("Producer").getOrCreate

  val df = spark.read.json("src/main/resources/_results/20150801-20151101-raw_user_activity-converted-0.json")
  df.printSchema()
  df.show(10)
  val orderedDF = df.orderBy("event_time")
  orderedDF.show(10)
  var tempDate = LocalDateTime.parse(orderedDF.first().getAs[String]("event_time"), ISO_DATE_TIME)

  val kafkaProps: Properties = new Properties()
  kafkaProps.put("bootstrap.servers", "localhost:9092")
  kafkaProps.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("acks", "all")
  val producer = new KafkaProducer[String, String](kafkaProps)


  orderedDF.rdd.collect().foreach(row => {
    val rowDate = LocalDateTime.parse(row.getAs("event_time"), ISO_DATE_TIME)
    val diff = rowDate.toEpochSecond(ZoneOffset.UTC) - tempDate.toEpochSecond(ZoneOffset.UTC)
    println(s"Will wait for $diff ...")
    Thread.sleep((diff * 1000 * timeFactor).toLong)
    tempDate = rowDate
    println(row)
    println("-----------")
    val key = s"${row.getAs("user_id")}${row.getAs("course_id")}"
    try {
      val record = new ProducerRecord[String, String](topic, key, row.json)
      val metadata = producer.send(record)
      printf(s"sent record(key=%s value=%s) " +
        "meta(partition=%d, offset=%d)\n",
        record.key(), record.value(),
        metadata.get().partition(),
        metadata.get().offset())

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      producer.close()
    }

  })

}