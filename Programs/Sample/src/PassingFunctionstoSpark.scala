import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object PassingFunctionstoSpark {
  def main(args: Array[String]) {
    val sc1 = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory", "1g");
    val sc = new SparkContext(sc1);
    val lines = sc.textFile("Sample.txt")
    var ndd = lines.map(MyClass.func1);
    ndd.foreach(println);

  }
}

object MyClass {
  def func1(s: String): String = { return s.toUpperCase(); }
}