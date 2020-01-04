package com.infoedge.analytics.sessionvalidation.executor

import com.infoedge.analytics.sessionvalidation.configuration.SessionValidationGlobalConfiguration
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel
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

  }

  /** getSessionDebugDF
   *
   * @param inputDF
   * @return
   */
  def getSessionAnalysisDF(inputDF: DataFrame): DataFrame = {

    val window = Window.partitionBy("e_visitor_id").orderBy("e_time")
    val lagTimeCol = lag(col("e_time"), 1).over(window)

    val lastSessionTimeDifference = col("e_time").cast("long") - lagTimeCol.cast("long")

    val newSession =  (coalesce(lastSessionTimeDifference/60, lit(0)) > 30).cast("bigint")

    val visitorSessionWindow = Window.partitionBy("e_visitor_id", "common_session").orderBy("e_time")

    // Just to make little informative and make analysis easy added all step column
    // we can directly just add indent_session field at it is only required condition to check
    val newDF = inputDF
      .withColumn("last_e_time", lagTimeCol)
      .withColumn("session_time_diff",lastSessionTimeDifference/60)
      .withColumn("common_session", sum(newSession).over(window))
      .withColumn("intended_session_id", first("e_session_id").over(visitorSessionWindow))
      .withColumn("valid_session", when(col("intended_session_id")=== col("e_session_id"), true).otherwise(false))
    newDF
  }


  /** getUniqueAnomalyVisitor
   *
   * @param analysisDF
   * @return
   */
  def getUniqueAnomalyVisitor(analysisDF: DataFrame): DataFrame = {
    analysisDF.filter(col("valid_session") === false).select("e_visitor_id").distinct()
  }

  /** getAnomalyExtraSession
   *
   * @param analysisDF
   * @return
   */
  def getAnomalyExtraSession(analysisDF: DataFrame): DataFrame = {
    analysisDF.filter(col("valid_session") === false).select("e_session_id", "e_visitor_id", "e_time").distinct()
  }

  /** getUpdatedSessionDFWithIntendedSessionId
   *
   * @param analysisDF
   * @return
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
