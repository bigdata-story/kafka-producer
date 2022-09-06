import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.io.File
import java.time.{ZoneId, ZoneOffset}
import java.util.Date
import scala.io.Source

object Producer extends App {

  import java.util.Properties
  import java.time.LocalDateTime
  import java.time.format.DateTimeFormatter._

  // every second will be factored in this number
  val timeFactor = scala.util.Properties.envOrElse("TIME_FACTOR", "0.001").toFloat
  println(timeFactor)
  // topic
  val eventByUserIdTopic = scala.util.Properties.envOrElse("EVENT_USER_ID", "events_by_user_id");
  val eventByUserIdCourseIdTopic = scala.util.Properties.envOrElse("EVENT_USER_ID_COURSE_ID", "events_by_user_id_course_id");
  val videoEventByUserIdCourseIdTopic = scala.util.Properties.envOrElse("VIDEO_EVENT_USER_ID_COURSE_ID", "video_events_by_user_id_course_id");
  val dataPath = scala.util.Properties.envOrElse("DATA_PATH", "src/main/resources")
  val bootstrapServers = scala.util.Properties.envOrElse("BOOTSTRAP_SERVERS", "localhost:9092")
  var timeout = scala.util.Properties.envOrElse("TIMEOUT", "1000").toInt
  val kafkaProps: Properties = new Properties()
  kafkaProps.put("bootstrap.servers", bootstrapServers)
  kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("acks", "all")
  val producer = new KafkaProducer[String, String](kafkaProps)
  while (timeout > 0){
    timeout -= 1
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
            val userId = s"${event("user_id").str}"
            val courseIdWithUserId = s"${event("user_id")}${event("course_id")}"
            event("event_time") = LocalDateTime.parse(LocalDateTime.now().toString, ISO_DATE_TIME).toString
            val tmp = event.render()
            try {
              producer.send(new ProducerRecord[String, String](eventByUserIdTopic, userId, tmp))
              producer.send(new ProducerRecord[String, String](eventByUserIdCourseIdTopic, courseIdWithUserId, tmp))
              val ls = List("pause_video", "play_video", "stop_video")
              if(ls.contains(event("event_type").str)) {
                producer.send(new ProducerRecord[String, String](videoEventByUserIdCourseIdTopic, courseIdWithUserId, tmp))
              }
              println("-----------")
            } catch {
              case e: Exception => e.printStackTrace()
            } finally {
            }
          }
        )
      }
    )
  }
  producer.close()
}