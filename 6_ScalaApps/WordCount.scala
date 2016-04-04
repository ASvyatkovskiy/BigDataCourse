import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._

object WordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("WordCountApp")
    val spark = new SparkContext(conf)

    val ipath_str = sys.env("WORKDIR_ROOT") 
    val textFile = spark.textFile("file://"+ipath_str+"/2_LoadingData/unstructured/",10)
    val counts = textFile.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)

    println("\nTaking the 10 most frequent words in the text and corresponding frequencies:")
    println(counts.takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2)))
        
    spark.stop()
  }
}
