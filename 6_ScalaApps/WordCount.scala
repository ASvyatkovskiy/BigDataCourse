import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.SparkContext._

/** Computes an approximation to pi */
object WordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)

    val prefix = "/user/alexeys/MaryTests/"
    val scheduler = Set(("CO","2004"),("CO","2005"),("NJ","2000"),("NJ","2002"),("IN","2003"))

    val t0 = System.nanoTime()

    for (stateyear1 <- scheduler) {
       val model_folder = prefix+"/bills/lexs/text2b/"+stateyear1._1+"/"+stateyear1._2+"/catalog_"+stateyear1._1+stateyear1._2
       val counts = spark.textFile(model_folder).flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
       counts.saveAsTextFile(prefix+"/matches/"+stateyear1._1+stateyear1._2)
    }

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000.)
    spark.stop()
  }
}
