package com.dapeng

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSQL {
  case class tb(year_month:String,college:String,org_flow_type:String,region:String,team:String,group_a:String,employee_id:String)
  def main(args: Array[String]): Unit = {

    //创建上下文环境配置对象
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkSQL")
    //创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //spark不是包名，是上下文环境对象名
    import spark.implicits._
    val value: Dataset[String] = spark.read.textFile("hdfs://node-1:8020/user/hive/warehouse/dwd_advisory_group_month_end_status")
    val value1: Dataset[tb] = value.map(rdd => {
      val strings: Array[String] = rdd.split("\\001")
      tb(strings(0), strings(1), strings(2), strings(3), strings(4), strings(5), strings(6))
    })
    val value2: Dataset[String] = spark.read.textFile("hdfs://node-1:8020/user/hive/warehouse/dwd_advisory_group_month_end_status_p_detail")
    val value3: Dataset[tb] = value2.map(rdd => {
      val strings: Array[String] = rdd.split("\\001")
      tb(strings(0), strings(1), strings(2), strings(3), strings(4), strings(5), strings(6))
    })

    value1.toDF().createOrReplaceTempView("event")
    value3.toDF().createOrReplaceTempView("event1")
    val frame: DataFrame = spark.sql("select t1.year_month,t2.college from event t1 left join event1 t2 on t1.team=t2.team and t1.college=t2.college and t1.region=t2.region where t1.college='设计学院'  ")
    frame.show()
//    value.rdd.saveAsTextFile(s"hdfs://hadoop102:8020/aabbcc/${NowDate()}")

  }

  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormat.format(now)
    return date
  }
}
