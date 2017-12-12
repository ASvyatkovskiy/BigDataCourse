name := "ScalExample"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    // Spark dependency
    "org.apache.spark"  % "spark-core_2.11" % "2.2.0" % "provided",
    "org.apache.spark"  % "spark-mllib_2.11" % "2.2.0" % "provided",
    "org.apache.spark"  % "spark-sql_2.11" % "2.2.0" % "provided"
)
