import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.apache.spark.sql.SparkSession

object UDAFExample { 

  class GeometricMean extends UserDefinedAggregateFunction {
    // This is the input fields for your aggregate function.
    override def inputSchema: org.apache.spark.sql.types.StructType = 
      StructType(StructField("value", DoubleType) :: Nil)

    // This is the internal fields you keep for computing your aggregate.
    override def bufferSchema: StructType = StructType(
      StructField("count", LongType) ::
      StructField("product", DoubleType) :: Nil
    )
  
    // This is the output type of your aggregatation function.
    override def dataType: DataType = DoubleType
  
    override def deterministic: Boolean = true
  
    // This is the initial value for your buffer schema.
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 1.0
    }
   
    // This is how to update your buffer schema given an input.
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getAs[Long](0) + 1
      buffer(1) = buffer.getAs[Double](1) * input.getAs[Double](0)
    }
 
    // This is how to merge two objects with the bufferSchema type.
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
      buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
    }

    // This is where you output the final value, given the final value of your bufferSchema.
    override def evaluate(buffer: Row): Any = {
      math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0))
    }
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
      (1, 0.5), (1, 0.3), (1, 2.1),
      (2, 0.7), (2, 1.1), (3, 3.2),
      (3, 0.2), (3, 7.1))).toDF("group_id","value")

    val gm = new GeometricMean

    // Show the geometric mean of values of column "id".
    val result = df.groupBy($"group_id").agg(gm($"value").as("GeometricMean"))
    result.show()
    result.printSchema()

  }
}
