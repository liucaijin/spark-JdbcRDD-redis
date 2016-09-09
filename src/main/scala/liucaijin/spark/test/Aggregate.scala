package liucaijin.spark.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Aggregate {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local[1]")
    val sc = new SparkContext(conf)
     val input=sc.parallelize(List(1,2,3,4,5))
//     val result = input.aggregate(zeroValue)(seqOp, combOp)
         val result = input.aggregate((0, 0))(
             (acc, value) => (acc._1 + value, acc._2 + 1),
             (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
         val avg = result._1 / result._2.toDouble
         println(avg)
         
  }
}