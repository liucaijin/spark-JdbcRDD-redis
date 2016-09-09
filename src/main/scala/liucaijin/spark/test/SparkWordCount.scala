package liucaijin.spark.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object SparkWordCount {

  def main(args: Array[String]) {
    //    if (args.length == 0) {     
    //    System.err.println("Usage: SparkWordCount <inputfile> <outputfile>")    
    //    System.exit(1)    
    //    }  
    val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val data  = sc.parallelize(List((1, 3), (1, 200), (1, 100), (2, 3), (2, 4), (2, 5)), 100)
    data.saveAsTextFile("")
    def seqOp(a: Int, b: Int): Int = {
      println("seq: " + a + "\t " + b)
      math.max(a, b)
    }
    def combineOp(a: Int, b: Int): Int = {
      println("comb: " + a + "\t " + b)
      a + b
    }
    //    val localIterator=data.aggregateByKey(0)((_,_)._2, _+_).collect();
    val localIterator = data.aggregateByKey(4)(seqOp, combineOp).collect();
    for (i <- localIterator) println(i)
    sc.stop()
  }
}