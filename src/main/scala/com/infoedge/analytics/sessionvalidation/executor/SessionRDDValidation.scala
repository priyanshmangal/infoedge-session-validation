package com.infoedge.analytics.sessionvalidation.executor

import com.infoedge.analytics.sessionvalidation.configuration.SessionValidationGlobalConfiguration
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.storage.StorageLevel

class SessionRDDValidation(sessionValidationGlobalConfiguration: SessionValidationGlobalConfiguration)
  extends Serializable {

  /**
   *
   * @param sqlContext
   */
  def execute(sqlContext: SQLContext): Unit = {

    val sessionValidationJob = SessionValidationJob(sessionValidationGlobalConfiguration)
    val inputDF = sessionValidationJob.readInputData(sqlContext)

    val analysisSessionDF = getSessionAnalysisDF(inputDF, sqlContext).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val uniqueAnomalyVisitorDF = sessionValidationJob.getUniqueAnomalyVisitor(analysisSessionDF)
    val anomalyExtraSessionDF = sessionValidationJob.getAnomalyExtraSession(analysisSessionDF)
    val updatedIntendedSessionIdDF = sessionValidationJob.getUpdatedSessionDFWithIntendedSessionId(analysisSessionDF)

    val outputConfig = sessionValidationGlobalConfiguration.output
    val outputFilePath = outputConfig.getString("path")

    sessionValidationJob.writeOutputDataFrame(uniqueAnomalyVisitorDF, outputFilePath + "/uniqueAnomalyVisitorDF")
    sessionValidationJob.writeOutputDataFrame(anomalyExtraSessionDF, outputFilePath + "/anomalyExtraSessionDF")
    sessionValidationJob.writeOutputDataFrame(updatedIntendedSessionIdDF, outputFilePath + "/updatedIntendedSessionIdDF")

  }


  /**
   *
   * @param inputDF
   * @param sqlContext
   * @return
   */
  def getSessionAnalysisDF(inputDF: DataFrame, sqlContext: SQLContext): DataFrame = {
    val inputSchema = inputDF.schema

    val visitorIdIndex = inputSchema.fieldIndex("e_visitor_id")
    val sessionTimeIndex = inputSchema.fieldIndex("e_time")
    val sessionIdIndex = inputSchema.fieldIndex("e_session_id")

    val groupedRDD = inputDF.rdd.groupBy { row =>
      val user = row.getLong(visitorIdIndex)
      user
    }

    val newRDD = groupedRDD.flatMap { case (_, rowList) =>

      val sortedRowList = rowList.toList.sortBy(row => row.getTimestamp(sessionTimeIndex).getTime)
      val initialRow = sortedRowList.head
      val initialSessionId = initialRow.getString(sessionIdIndex)
      val updatedFirstRow = Row.fromSeq(initialRow.toSeq ++ Seq(initialSessionId))

      val (newIntendedRowList, _) = sortedRowList.sliding(2).foldLeft(List(updatedFirstRow), initialSessionId) {
        case ((updatedRowList, currentSessionId), slidingRowList) =>

          val (updatedRow, updatedCurrentSessionId) = if (slidingRowList.size == 1) {
            val firstRow = slidingRowList.head
            (Row.fromSeq(firstRow.toSeq ++ Seq(currentSessionId)), currentSessionId)
          }
          else {
            val firstRow = slidingRowList.head
            val secondRow = slidingRowList.last
            val timeDiff = (secondRow.getTimestamp(sessionTimeIndex).getTime - firstRow.getTimestamp(sessionTimeIndex).getTime) / (60 * 1000.0)
            if (timeDiff < 30) {
              (Row.fromSeq(secondRow.toSeq ++ Seq(currentSessionId)), currentSessionId)
            } else {
              val secondRowSessionId = secondRow.getString(sessionIdIndex)
              (Row.fromSeq(secondRow.toSeq ++ Seq(secondRowSessionId)), secondRowSessionId)
            }
          }
          val appededRowList = updatedRowList :+ updatedRow
          (appededRowList, updatedCurrentSessionId)
      }
      newIntendedRowList
    }

    val newSchema = StructType(inputSchema.fields ++ Array(StructField("intended_session_id", StringType)))

    val newDF = sqlContext.createDataFrame(newRDD, newSchema)
    newDF.withColumn("valid_session", when(col("intended_session_id") === col("e_session_id"), true).otherwise(false))


  }
}

/** Companion Object for SessionRDDValidation */
object SessionRDDValidation {
  def apply(dataValidationGlobalConfiguration:
            SessionValidationGlobalConfiguration): SessionRDDValidation
  = new SessionRDDValidation(dataValidationGlobalConfiguration)
}

