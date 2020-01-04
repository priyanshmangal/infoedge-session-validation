package com.infoedge.analytics.sessionvalidation.utility.sharedcontext

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * SharedSparkContext
  *
  */
trait SharedSparkContext extends BeforeAndAfterAll {
  this: Suite =>

  private var _sc: SparkContext = _
  private var _sqlContext: SQLContext = _

  /** Load the default SparkConf and set master as local.
    * Can be used to enable kryo support and register kryo classes in some test suites.
    *
    * Any Suite which wants to add extra configuration should override this in beforeAll style.
    * eg: override def sparkConf = super.sparkConf.set("spark.master",  "local[1]")
    *
    * @return SparkConf
    */
  def sparkConf: SparkConf =
    new SparkConf()
    .setMaster("local[*]")
    .setAppName(this.getClass.getSimpleName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    _sc = new SparkContext(sparkConf)
    _sqlContext = new SQLContext(_sc)
    Logger.getLogger("org").setLevel(Level.ERROR)
  }

  override def afterAll(): Unit = {
    if (_sc != null) {
      _sqlContext = null
      _sc.stop()
      _sc = null
    }
    super.afterAll()
  }

  def sc: SparkContext = _sc

  def sqlContext: SQLContext = _sqlContext
}

