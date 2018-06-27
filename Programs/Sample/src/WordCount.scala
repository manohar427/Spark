
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc1 = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory", "1g");
    val sc = new SparkContext(sc1);

    val textFile = sc.textFile("Sample.txt")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("xyz")
    println("OK");
  }
}