import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.apache.spark.sql.SparkSession

object DatasetAggExample {

  class GeometricMean extends Aggregator[AggDocument, (Long,Double), (Double)] {

     //zero for aggregation. Should have the type of the buffer
     def zero: (Long,Double) = (0L,1.0)
     //aggregation operation within each partition
     def reduce(buffer: (Long,Double), doc: AggDocument): (Long,Double) = {
        (buffer._1+1,buffer._2*doc.value)
     }
     //merge partitions
     def merge(buffer1: (Long,Double), buffer2: (Long,Double)): (Long,Double) = {
        (buffer1._1+buffer2._1,buffer1._2+buffer2._2)
     }
     //additional operations on the result
     def finish(r: (Long,Double)): Double = math.pow(r._2, 1.toDouble / r._1) 

     //encoders 
     override def bufferEncoder: Encoder[(Long,Double)] = ExpressionEncoder()
     override def outputEncoder: Encoder[Double] = ExpressionEncoder()
  }

  def main (args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("BillAnalysis")
      .config("spark.dynamicAllocation.enabled","true")
      .config("spark.shuffle.service.enabled","true")
      .config("spark.sql.codegen.wholeStage", "true")
      .getOrCreate()

    import spark.implicits._

    val df = spark.createDataFrame(Seq(
      (1, "Happy", 0.5), (1, "Happy", 0.3), (1, "Happy", 2.1),
      (2, "Fluffy", 0.7), (2, "Fluffy", 1.1), (3,"Sad", 3.2),
      (3, "Sad", 0.2), (3,"Sad", 7.1))).toDF("group_id","property","value")
    val dataset = df.as[AggDocument]

    val gm = new GeometricMean

    // Show the geometric mean of values of column "id".
    val result = dataset.groupByKey(_.group_id).agg(gm.toColumn)
    result.show()
    result.printSchema()

  }
}
case class AggDocument(group_id: Int, property: String, value: Double)
