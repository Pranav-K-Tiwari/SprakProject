package learn

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import twitter4j.{FilterQuery, StallWarning, Status, StatusDeletionNotice, StatusListener, TwitterStream, TwitterStreamFactory}
import twitter4j.conf.ConfigurationBuilder

case class TwitterData(timestamp:Long, hashTags:Array[String])

object TwitterKafkaProducer {

  def getProducerObject(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    return new KafkaProducer[String, String](props)
  }

  def connectToTwitter(): TwitterStream = {
    val consumerKey = "QGYUkFHX4guxn0t1B3Pq4BPYu"
    val consumerSecret = "8yPbrYhqbi2VXHsZTZ27DZJhBMx0ZBfQk3P2XtqguPesfs8Ioo"
    val accessToken = "2148607874-SORqD2SgMttE23VlmzHV1xZVjZS5JrA7pz25VgK"
    val accessTokenSecret = "4tYxQ5jYfLT90wbBwV5xgQmYGyTmT1Emsxf69Lf6Dnohi"

    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
      .setJSONStoreEnabled(true)
      .setIncludeEntitiesEnabled(true)

    val streamFactory = new TwitterStreamFactory(cb.build()).getInstance()
    return streamFactory;
  }

  def main(args: Array[String]): Unit = {
    val topicName = "twitter-data1";
    val kafkaProducer = getProducerObject()
    val streamFactory = connectToTwitter();

    val listener = new StatusListener {
      override def onStatus(status: Status): Unit = {

        val hasTags = status.getHashtagEntities
        if(hasTags.length==0) return ;

        var str = "";
        val timeStamp = status.getCreatedAt.getTime;
        var jsonString = "{\"timeStamp\":" + timeStamp +",\"hashTags\":["

        hasTags.foreach(t => {
          str = str + "\""+ t.getText + "\","
        })

        str = str.substring(0, str.length-1) + "]}"
        jsonString = jsonString + str

        val producerRecord = new ProducerRecord[String, String](topicName, status.getCreatedAt.getTime.toString, jsonString)
        kafkaProducer.send(producerRecord);
      }

      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = ???

      override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = ???

      override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = ???

      override def onStallWarning(warning: StallWarning): Unit = ???

      override def onException(ex: Exception): Unit = ???
    }

    streamFactory.addListener(listener)

    val eastLongitude = 97.5
    val westLongitude = 68.7;

    val northLatitude = 37.6;
    val southLatitude = 8.4;

    val locationsIndia = Array(Array(westLongitude, southLatitude), Array(eastLongitude, northLatitude))

    val fq = new FilterQuery();
    fq.locations(locationsIndia)

    streamFactory.filter(fq)
  }
}
