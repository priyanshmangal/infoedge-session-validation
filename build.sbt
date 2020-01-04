
name := "session-validation"

version := "0.1"

scalaVersion := "2.11.0"

val sparkVersion = "2.4.4"


// Exclusion Rules for dependencies
val exclusionRule = List(
  ExclusionRule(organization = "javax.servlet", name = "servlet-api"),
  ExclusionRule(organization = "org.mortbay.jetty", name = "jetty"),
  ExclusionRule(organization = "org.mortbay.jetty", name = "servlet-api-2.5")
)

// Spark Library
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion excludeAll
  ExclusionRule(organization = "org.eclipse.jetty")
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

// Source and Sink Library
libraryDependencies += "com.databricks" %% "spark-csv" % "1.3.0"


libraryDependencies += "org.scalaj" % "scalaj-http_2.10" % "2.3.0"

libraryDependencies += "com.typesafe" % "config" % "1.3.4"

libraryDependencies += "joda-time" % "joda-time" % "2.9.4"


// Scala Unit Test Library
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0"

libraryDependencies += "net.liftweb" % "lift-json_2.10" % "2.6.2"
