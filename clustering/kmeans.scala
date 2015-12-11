import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext


object KMeans {
    def main(args: Array[String]) = {
        val sc = new SparkContext()
        def toLatLon(line: String) = {
            val latlon = ...
            Vectors.dense(latlon.map(v => v.toDouble))
        }
        val numIterations = 20
        val numClusters = Array(10, 50, 100)
        var dataFiles = Array("/scratch/network/alexeys/BigDataCourse/NYCtaxi/yellow_250t.csv")
    
        for (numCluster <- numClusters) {
            for (dataFile <- dataFiles) {
                // Process the data
                val d = sc.textFile(dataFile,15)

                // Remove first line
                val data = d.filter(line => ! line.contains("vendor_id"))
                val latlon = data.map(...).filter(v => v.size == 2).filter(v => v(0) != 0.0).cache()

                // Use Kmeans
                var start_time = System.currentTimeMillis()
                var clusters = KMeans.train(...)
                var end_time = System.currentTimeMillis()
                var WSSSE = clusters.computeCost(latlon)
                println("Within Set Sum of Squared Errors = " + WSSSE)
    
                clusters.toPMML(sc,"./output_kmeans_"+end_time.toString)
                println(s"Running KMeans with $numCluster clusters, we took ${end_time - start_time} ms")
            }
        }
                
    }
}
