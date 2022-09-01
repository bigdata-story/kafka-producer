import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.io.File
import java.time.ZoneOffset
import scala.io.Source

object Producer extends App {

  import java.util.Properties
  import java.time.LocalDateTime
  import java.time.format.DateTimeFormatter._

  // every second will be factored in this number
  val timeFactor = scala.util.Properties.envOrElse("TIME_FACTOR", "0.001").toFloat
  println(timeFactor)
  // topic
  val topic = scala.util.Properties.envOrElse("TOPIC", "events_topic")
  val dataPath = scala.util.Properties.envOrElse("DATA_PATH", "src/main/resources")
  val bootstrapServers = scala.util.Properties.envOrElse("BOOTSTRAP_SERVERS", "localhost:9092")
  val kafkaProps: Properties = new Properties()
  kafkaProps.put("bootstrap.servers", bootstrapServers)
  kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("acks", "all")
  val producer = new KafkaProducer[String, String](kafkaProps)
  LazyList.from(new File(dataPath).listFiles).filter(_.isFile).sorted.map(Source.fromFile).foreach(
    source => {
      var tempDate: LocalDateTime = null
      source.bufferedReader().lines().forEach(
        line => {
          val event = ujson.read(line).obj
          val rowDate = LocalDateTime.parse(event("event_time").str, ISO_DATE_TIME)
          if (tempDate == null) {
            tempDate = rowDate
          }
          val diff = rowDate.toEpochSecond(ZoneOffset.UTC) - tempDate.toEpochSecond(ZoneOffset.UTC)
          println(s"Will wait for $diff ...")
          Thread.sleep((diff * 1000 * timeFactor).toLong)
          tempDate = rowDate
          val key = s"${event("user_id").str}${event("course_id").str}"
          try {
            val record = new ProducerRecord[String, String](topic, key, line)
            val metadata = producer.send(record)
            printf(s"sent record(key=%s value=%s) " +
              "meta(partition=%d, offset=%d)\n",
              record.key(), record.value(),
              metadata.get().partition(),
              metadata.get().offset()
            )
            println("-----------")

          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
          }
        }
      )
    }
  )
  producer.close()
}