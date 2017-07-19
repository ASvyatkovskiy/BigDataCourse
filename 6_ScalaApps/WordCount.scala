import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("WordCount")
      .config("spark.sql.codegen.wholeStage", "true")
      .getOrCreate()

    import spark.implicits._

    val textFile = spark.sparkContext.textFile("file:///scratch/network/alexeys/BigDataCourse/unstructured/",10)
    val counts = textFile.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)

    println("\nTaking the 10 most frequent words in the text and corresponding frequencies:")
    println(counts.takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2)))
        
    spark.stop()
  }
}
