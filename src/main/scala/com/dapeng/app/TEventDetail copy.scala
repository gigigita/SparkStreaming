package com.dapeng.app

import java.util.Properties

import com.dapeng.RealTimeApp.prop
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object TEventDetail {

  def saveToMysql(spark:SparkSession,url:String,table:String,prop:Properties): Unit ={
    val resultDF: DataFrame = spark.sql("select * from event ")
    resultDF.show()
    resultDF.write.mode(SaveMode.Append).jdbc(url, table, prop)

  }
}
