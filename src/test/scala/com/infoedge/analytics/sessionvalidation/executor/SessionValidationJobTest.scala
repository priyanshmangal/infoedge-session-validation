package com.infoedge.analytics.sessionvalidation.executor

import java.sql.Timestamp

import com.infoedge.analytics.sessionvalidation.configuration.SessionValidationGlobalConfiguration
import com.infoedge.analytics.sessionvalidation.utility.sharedcontext.SharedSparkContext
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSuite


class SessionValidationJobTest extends FunSuite with SharedSparkContext {


  val applicationConfig = ConfigFactory.parseResources("analytics.conf")
  val sessionValidationGlobalConfiguration = new SessionValidationGlobalConfiguration(applicationConfig)

  test("SessionValidationJobTest - Jun Job") {

    SessionValidationJob(sessionValidationGlobalConfiguration).runJob(sqlContext)

  }

  test("SessionValidationJobTest - execute") {

    SessionValidationJob(sessionValidationGlobalConfiguration).execute(sqlContext)

  }


  test("SessionValidationJobTest - validate session and analysis") {

    val rowList = Seq(
      Row("aa", 1L, Timestamp.valueOf("2020-01-04 10:30:00")),
      Row("ab", 1L, Timestamp.valueOf("2020-01-04 10:15:00")),
      Row("ac", 1L, Timestamp.valueOf("2020-01-04 10:50:00")),
      Row("ba", 2L, Timestamp.valueOf("2020-01-04 10:20:00")),
      Row("bb", 2L, Timestamp.valueOf("2020-01-04 11:00:00")),
      Row("ca", 3L, Timestamp.valueOf("2020-01-04 12:10:00")),
      Row("cb", 3L, Timestamp.valueOf("2020-01-04 11:50:00")),
      Row("cc", 3L, Timestamp.valueOf("2020-01-04 11:55:00")),
      Row("cc", 3L, Timestamp.valueOf("2020-01-04 11:40:30")),
      Row("cd", 3L, Timestamp.valueOf("2020-01-04 12:50:30")),
      Row("ce", 3L, Timestamp.valueOf("2020-01-04 13:50:30"))

    )
    val rowSchema = StructType(
      Array(
        StructField("e_session_id", StringType),
        StructField("e_visitor_id", LongType),
        StructField("e_time", TimestampType)
      )
    )

    val rdd = sc.parallelize(rowList)
    val df = sqlContext.createDataFrame(rdd, rowSchema)

    val resultDF = SessionValidationJob(sessionValidationGlobalConfiguration).getSessionAnalysisDF(df)
    val actualDF =  resultDF.select("e_session_id", "e_visitor_id", "e_time", "intended_session_id", "valid_session")
    actualDF.printSchema()
    actualDF.show()

    val expectedRowList = Seq(
      Row("ab", 1L, Timestamp.valueOf("2020-01-04 10:15:00"), "ab", true),
      Row("aa", 1L, Timestamp.valueOf("2020-01-04 10:30:00"), "ab", false),
      Row("ac", 1L, Timestamp.valueOf("2020-01-04 10:50:00"), "ab", false),
      Row("cc", 3L, Timestamp.valueOf("2020-01-04 11:40:30"), "cc", true),
      Row("cb", 3L, Timestamp.valueOf("2020-01-04 11:50:00"), "cc", false),
      Row("cc", 3L, Timestamp.valueOf("2020-01-04 11:55:00"), "cc", true),
      Row("ca", 3L, Timestamp.valueOf("2020-01-04 12:10:00"), "cc", false),
      Row("cd", 3L, Timestamp.valueOf("2020-01-04 12:50:30"), "cd", true),
      Row("ce", 3L, Timestamp.valueOf("2020-01-04 13:50:30"), "ce", true),
      Row("ba", 2L, Timestamp.valueOf("2020-01-04 10:20:00"), "ba", true),
      Row("bb", 2L, Timestamp.valueOf("2020-01-04 11:00:00"), "bb", true)
    )
    val expectedRowSchema = StructType(
      Array(
        StructField("e_session_id", StringType, true),
        StructField("e_visitor_id", LongType, true),
        StructField("e_time", TimestampType, true),
        StructField("intended_session_id", StringType, true),
        StructField("valid_session", BooleanType, false)
      )
    )

    val expectedRdd = sc.parallelize(expectedRowList)
    val expectedDF = sqlContext.createDataFrame(expectedRdd, expectedRowSchema)
    assert(actualDF.schema === expectedDF.schema)
    assert(actualDF.rdd.collect().toSeq === expectedDF.rdd.collect().toSeq)
  }

  test("test - analysis") {
    val readDF = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true)
      .load("/Users/priyanshmangal/Documents/Personal-projects/data/input/")
    readDF.printSchema()
    readDF.show(false)
    val sessionValidation = SessionValidationJob(sessionValidationGlobalConfiguration)
    val analysisSessionDF = sessionValidation.getSessionAnalysisDF(readDF).cache()

    //    println(analysisSessionDF.filter(analysisSessionDF("final_validation")===false).count())
    //    println(analysisSessionDF.filter(analysisSessionDF("valid_session")===false).count())
    //
    //    analysisSessionDF.filter(analysisSessionDF("final_validation")===false &&
    //      analysisSessionDF("valid_session")===true).show(false)

    val uniqueAnomalyVisitorDF = sessionValidation.getUniqueAnomalyVisitor(analysisSessionDF)
    val anomalyExtraSessionDF = sessionValidation.getAnomalyExtraSession(analysisSessionDF)
    val updatedIntendedSessionIdDF = sessionValidation.getUpdatedSessionDFWithIntendedSessionId(analysisSessionDF)

     println(uniqueAnomalyVisitorDF.count())
    updatedIntendedSessionIdDF.show(50, false)
  }
}
