package com.infoedge.analytics.sessionvalidation.executor

import java.sql.Timestamp

import com.infoedge.analytics.sessionvalidation.configuration.SessionValidationGlobalConfiguration
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}


/** SessionValidationJob
 *
 */

class SessionValidationJob(sessionValidationGlobalConfiguration: SessionValidationGlobalConfiguration) extends Serializable {

  // using logger for logging any information of my job execution
  val logger: Logger = LoggerFactory.getLogger(getClass)

  lazy private val jobExecutionTime: String = s"exec-${System.currentTimeMillis()}"

  logger.info("Spark Job Run System Time -" + jobExecutionTime)

  def runJob(sqlContext: SQLContext): Unit = {
    execute(sqlContext)
  }

  /** Execute
   * Session Validation and Intend New Session Id
   *
   * @param sqlContext : sqlContext
   */
  def execute(sqlContext: SQLContext): Unit = {

    val inputDF = readInputData(sqlContext)

    val analysisSessionDF = getSessionAnalysisDF(inputDF).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val uniqueAnomalyVisitorDF = getUniqueAnomalyVisitor(analysisSessionDF)
    val anomalyExtraSessionDF = getAnomalyExtraSession(analysisSessionDF)
    val updatedIntendedSessionIdDF = getUpdatedSessionDFWithIntendedSessionId(analysisSessionDF)

    val outputConfig = sessionValidationGlobalConfiguration.output
    val outputFilePath = outputConfig.getString("path")

    writeOutputDataFrame(uniqueAnomalyVisitorDF, outputFilePath+ "/uniqueAnomalyVisitorDF")
    writeOutputDataFrame(anomalyExtraSessionDF, outputFilePath+ "/anomalyExtraSessionDF")
    writeOutputDataFrame(updatedIntendedSessionIdDF, outputFilePath+ "/updatedIntendedSessionIdDF")

    logger.info("Spark Job End Time -" + System.currentTimeMillis())
  }


  /**
   *  udf to get minutes between two timestamp
   */
  val validate: UserDefinedFunction = udf { (first: Timestamp, second: Timestamp ) =>
      val timeDiff = (new DateTime(first).getMillis - new DateTime(second).getMillis) / (60 * 1000.0)
      timeDiff
  }

  /** getSessionAnalysisDF
   * Identify all sessions which are invalide for criterion:
   * There should be at least 30 minutes difference in any two sessions by any visitor.
   *
   * get updated data frame which carry analytics information:
   * time differnce between two sessions, new intented session id and valid session flag
   *
   * @param inputDF
   * @return analysis data frame
   */
  def getSessionAnalysisDF(inputDF: DataFrame): DataFrame = {

    val window = Window.partitionBy("e_visitor_id").orderBy("e_time")
    val lagTimeCol = lag(col("e_time"), 1).over(window)

    val lastSessionTimeDifference = when(lagTimeCol.isNull, null).otherwise(validate(col("e_time"), lagTimeCol))

    val newSession =  (coalesce(lastSessionTimeDifference, lit(0)) >= 30).cast("bigint")

    val visitorSessionWindow = Window.partitionBy("e_visitor_id", "common_session").orderBy("e_time")

    // Just to make little informative and make analysis easy added all step column
    // we can directly just add indent_session field at it is only required condition to check
    val newDF = inputDF
      .withColumn("last_e_time", lagTimeCol)
      .withColumn("session_time_diff",lastSessionTimeDifference)
      .withColumn("common_session", sum(newSession).over(window))
      .withColumn("intended_session_id", first("e_session_id").over(visitorSessionWindow))
      .withColumn("valid_session", when(col("intended_session_id")=== col("e_session_id"), true).otherwise(false))
    newDF
  }



  /** getUniqueAnomalyVisitor
   * find all anomaly unique visitor which have wrong session created.
   *
   * @param analysisDF
   * @return dataframe with visitor id
   */
  def getUniqueAnomalyVisitor(analysisDF: DataFrame): DataFrame = {
    analysisDF.filter(col("valid_session") === false).select("e_visitor_id").distinct()
  }

  /** getAnomalyExtraSession
   * find all session which are not as per criterion least 30 minutes difference in any two sessions by any visitor.
   *
   * @param analysisDF
   * @return dataframe with anomaly session details
   */
  def getAnomalyExtraSession(analysisDF: DataFrame): DataFrame = {
    analysisDF.filter(col("valid_session") === false).select("e_session_id", "e_visitor_id", "e_time").distinct()
  }

  /** getUpdatedSessionDFWithIntendedSessionId
   * get new intended session id for given data
   *
   * @param analysisDF
   * @return dataframe with intended session id
   */
  def getUpdatedSessionDFWithIntendedSessionId (analysisDF: DataFrame): DataFrame = {
    analysisDF.select("e_session_id", "e_visitor_id", "e_time", "intended_session_id")
  }



  /** writeOutputDataFrame
   *
   * @param outputDF Output DataFrame
   * @param filePath File Path
   */
  def writeOutputDataFrame(outputDF: DataFrame, filePath: String): Unit = {
    outputDF
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .save(filePath)
  }

  /** readInputData : Read Input Dataframe
   *
   * @param sqlContext SQL Context
   * @return data frame
   */
  def readInputData(sqlContext: SQLContext): DataFrame = {

    val inputConfig = sessionValidationGlobalConfiguration.input
    val filePath = inputConfig.getString("path")
    val inferSchema = if (inputConfig.hasPath("inferSchema")) {
      inputConfig.getBoolean("inferSchema")
    } else {
      true
    }
    val resourceFile = if (inputConfig.hasPath("resource")) {
      inputConfig.getBoolean("resource")
    } else {
      false
    }
    val updatedFilePath = if (resourceFile) {
      getClass.getResource(filePath).getFile
    } else {
      filePath
    }
    val readDF = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", inferSchema)
      .load(updatedFilePath)

    readDF
  }

}

/** Companion Object for SessionValidationJob */
object SessionValidationJob {
  def apply(dataValidationGlobalConfiguration:
            SessionValidationGlobalConfiguration): SessionValidationJob
  = new SessionValidationJob(dataValidationGlobalConfiguration)
}
