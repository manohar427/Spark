import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object BasicTest {
  def main(args: Array[String]) {
    val sc1 = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory", "1g");
    val sc = new SparkContext(sc1);

    val lines = sc.textFile("Sample.txt")
    // val lineLengths = lines.map(s => s.length)
    // val lineLengths = lines.map(s => s.toUpperCase())
    val lineLengths = lines.map(s => s.toLowerCase())
    println("DATA 1");
    lineLengths.foreach(println);
    println("DATA 1");
    val totalLength = lineLengths.reduce((a, b) => a + b)
    println(totalLength);
  }
}