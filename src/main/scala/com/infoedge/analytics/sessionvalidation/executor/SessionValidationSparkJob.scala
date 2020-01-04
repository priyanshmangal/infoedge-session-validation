package com.infoedge.analytics.sessionvalidation.executor

import com.infoedge.analytics.sessionvalidation.configuration.SessionValidationGlobalConfiguration
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object SessionValidationSparkJob {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("data_validation")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    val dataValidationGlobalConfiguration = SessionValidationGlobalConfiguration("analytics")

    SessionValidationJob(dataValidationGlobalConfiguration).runJob(sqlContext)
  }

}
