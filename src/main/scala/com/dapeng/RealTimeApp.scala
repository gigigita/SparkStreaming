package com.dapeng
import java.sql.Connection
import java.util.Properties

import com.dapeng.app.{TEventDetail, TEventDetailEventCountDay, UserLoginOnSignUpDay}
import com.dapeng.util.{JsonToBean, MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
object RealTimeApp {
  val prop = new Properties()
    prop.setProperty("driver", PropertiesUtil.load("config.properties").getProperty("driver"))
    prop.setProperty("user", PropertiesUtil.load("config.properties").getProperty("jdbc.user"))
    prop.setProperty("password", PropertiesUtil.load("config.properties").getProperty("jdbc.password"))

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealTimeApp")
    sparkConf.set("spark.streaming.backpressure.enabled", "true")
             .set("spark.streaming.stopGracefullyOnShutdown", "true")
      //控制每秒读取Kafka每个Partition最大消息数/second，若Streaming批次为3秒，topic最大分区为3，则每3s批次最大接收消息数为3*3*1000=9000/bach
//             .set("spark.streaming.kafka.maxRatePerPartition", "1000")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //2.创建StreamingContext
    val ssc = new StreamingContext(sc, Seconds(3))
    ssc.sparkContext.setCheckpointDir("eventCheck")

    //3.读取Kafka数据
    val topic: String = PropertiesUtil.load("config.properties").getProperty("kafka.topic")
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc)

    kafkaDStream.map(ds=>JsonToBean.gson(ds.value())).foreachRDD(rdd=>{
      val spark: SparkSession = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      val df = spark.createDataFrame(rdd)
      df.createOrReplaceTempView("event")
      df.cache()
      val url=PropertiesUtil.load("config.properties").getProperty("jdbc.url")
      try{
        TEventDetail.saveToMysql(spark,url,"t_event_detail",prop)
        TEventDetailEventCountDay.saveToMysql(spark,url,"t_event_detail_event_count_day")
        UserLoginOnSignUpDay.saveToMysql(spark,url,"user_login_on_sign_up_day")
      }catch {
        case e:Exception =>{print(e)}
      }


    })


    ssc.start()
    ssc.awaitTermination()

  }
}