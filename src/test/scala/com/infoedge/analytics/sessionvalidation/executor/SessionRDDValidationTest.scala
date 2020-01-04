package com.infoedge.analytics.sessionvalidation.executor

import java.sql.Timestamp

import com.infoedge.analytics.sessionvalidation.configuration.SessionValidationGlobalConfiguration
import com.infoedge.analytics.sessionvalidation.utility.sharedcontext.SharedSparkContext
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField, StructType, TimestampType}
import org.scalatest.FunSuite

class SessionRDDValidationTest extends FunSuite  with SharedSparkContext {

  val applicationConfig = ConfigFactory.parseResources("analytics.conf")
  val sessionValidationGlobalConfiguration = new SessionValidationGlobalConfiguration(applicationConfig)

  test("SessionRDDValidationTest - validate session and analysis") {

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

    val resultDF = SessionRDDValidation(sessionValidationGlobalConfiguration).getSessionAnalysisDF(df, sqlContext)
    val actualDF =  resultDF
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
    assert(actualDF.except(expectedDF).count == 0)
    assert(expectedDF.except(actualDF).count == 0)

  }


  test("assert between rdd level validation and dataframe level validation") {
    val readDF = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true)
      .load("/Users/priyanshmangal/Documents/Personal-projects/data/input/")
    val actualDF = readDF.filter(readDF("e_visitor_id") === 667366160187382810L)
    actualDF.printSchema()
    actualDF.show(false)
    val sessionDFValidation = SessionValidationJob(sessionValidationGlobalConfiguration)
    val analysisSessionDF = sessionDFValidation.getSessionAnalysisDF(actualDF).cache()
    val uniqueAnomalyVisitorDF = sessionDFValidation.getUniqueAnomalyVisitor(analysisSessionDF)
    val anomalyExtraSessionDF = sessionDFValidation.getAnomalyExtraSession(analysisSessionDF)
    val updatedIntendedSessionIdDF = sessionDFValidation.getUpdatedSessionDFWithIntendedSessionId(analysisSessionDF)
     analysisSessionDF.show(100, false)

    val sessionRDDValidation = SessionRDDValidation(sessionValidationGlobalConfiguration)
    val analysisRDDSessionDF = sessionRDDValidation.getSessionAnalysisDF(actualDF, sqlContext).cache()
    val uniqueRDDAnomalyVisitorDF = sessionDFValidation.getUniqueAnomalyVisitor(analysisRDDSessionDF).cache()
    val anomalyRDDExtraSessionDF = sessionDFValidation.getAnomalyExtraSession(analysisRDDSessionDF)
    val updatedRDDIntendedSessionIdDF = sessionDFValidation.getUpdatedSessionDFWithIntendedSessionId(analysisRDDSessionDF)

     analysisRDDSessionDF.show(100, false)
//    uniqueAnomalyVisitorDF.except(uniqueRDDAnomalyVisitorDF).show(false)
//    uniqueRDDAnomalyVisitorDF.except(uniqueAnomalyVisitorDF).show(false)
//    assert(uniqueAnomalyVisitorDF.except(uniqueRDDAnomalyVisitorDF).count == 0)

  }
}
