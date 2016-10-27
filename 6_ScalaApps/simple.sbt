name := "ScalExample"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
    // Spark dependency
    "org.apache.spark"  % "spark-core_2.11" % "2.0.0" % "provided"
)
