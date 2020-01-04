package com.infoedge.analytics.sessionvalidation.configuration

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}


class SessionValidationGlobalConfiguration(config: Config) extends Serializable {

  val input: Config = config.getConfig("input")
  val output: Config = config.getConfig("output")

}

object SessionValidationGlobalConfiguration {
  def apply(source: String): SessionValidationGlobalConfiguration = {
    val analyticsConfig = ConfigFactory.load(source)
    new SessionValidationGlobalConfiguration(analyticsConfig)
  }
}


