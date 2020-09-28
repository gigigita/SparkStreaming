package com.dapeng.app

import java.util.{Date, Properties}

import com.dapeng.util.PropertiesUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object TEventDetailEventCountDay {
  def saveToMysql(spark:SparkSession,url:String,table:String): Unit ={
    val resultDF: DataFrame = spark.sql(
      "select to_date(event_time)  event_date,event_theme,event_id,count(1) as count from event where event_time is not null group by 1,2,3 ")
    resultDF.show()
    val rdd2: RDD[Row] = resultDF.rdd


    rdd2.foreachPartition(rddP=> {


      val driver = PropertiesUtil.load("config.properties").getProperty("driver")
      val url = PropertiesUtil.load("config.properties").getProperty("jdbc.url")
      val userName = PropertiesUtil.load("config.properties").getProperty("jdbc.user")
      val passWd = PropertiesUtil.load("config.properties").getProperty("jdbc.password")
      Class.forName(driver)
      val connection = java.sql.DriverManager.getConnection(url, userName, passWd)
      connection.setAutoCommit(false)
      val statement = connection.createStatement()

      rddP.foreach(cc => {
        val event_date: Date = cc.getDate(0)
        val event_theme: String = cc.getString(1)
        val event_id: String = cc.getString(2)
        val count: Long = cc.getLong(3)

        val sql1 = s"""|insert into ${table}
                       |(event_date,event_theme,event_id,count)
                       |values
                       |('${event_date}','${event_theme}','${event_id}','${count}')
                       |on duplicate key update count=count+values(count);
                     """.stripMargin
        statement.addBatch(sql1)

      })

      statement.executeBatch()
      connection.commit()
      connection.close()
      statement.close()
    })
  }
}
