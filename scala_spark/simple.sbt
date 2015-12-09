name := "WordCount"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
    // Spark dependency
    "org.apache.spark" % "spark-core_2.10" % "1.4.1" % "provided"
)
