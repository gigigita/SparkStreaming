package com.dapeng.app

import java.util.{Date, Properties}

import com.dapeng.util.PropertiesUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object UserLoginOnSignUpDay {


  def saveToMysql(spark:SparkSession,url:String,table:String): Unit ={
    val resultDF: DataFrame = spark.sql("select " +
      "to_date(event_time)  event_date," +
      "sum(case when event_id='ztxx_dl' and extend7 ='SUCCESS' then 1 else 0 end ) login_on,\n" +
      "sum(case when event_id='ztxx_zc' and (event_target_spare1 ='SUCCESS' or event_target_spare1='注册成功' or event_target_spare2 ='SUCCESS') then 1 else 0 end ) sign_up\n" +
      "from event  \n" +
      "group by 1 ")

    resultDF.show()
    val rdd2: RDD[Row] = resultDF.rdd

    rdd2.foreach(rdd=>{

      val driver = PropertiesUtil.load("config.properties").getProperty("driver")
      val url = PropertiesUtil.load("config.properties").getProperty("jdbc.url")
      val userName = PropertiesUtil.load("config.properties").getProperty("jdbc.user")
      val passWd = PropertiesUtil.load("config.properties").getProperty("jdbc.password")
      Class.forName(driver)
      val connection = java.sql.DriverManager.getConnection(url, userName, passWd)
      connection.setAutoCommit(false)
      val statement= connection.createStatement()

      val event_date: Date = rdd.getDate(0)
      val login_on_num: Long = rdd.getLong(1)
      val sign_up_num: Long = rdd.getLong(2)
      val sql1=
        s"""
           |insert into ${table}  values ('${event_date}','${login_on_num}','${sign_up_num}')
           |on duplicate key update login_on_num=login_on_num+values(login_on_num),sign_up_num=sign_up_num+values(sign_up_num);
           |""".stripMargin
      statement.addBatch(sql1)
      statement.executeBatch()
      connection.commit()
      connection.close()
      statement.close()

    })
  }


}
