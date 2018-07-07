
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object JsonDemo {
  
  def main(args: Array[String]) {
    val sc1 = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory", "1g");
    val sc = new SparkContext(sc1);

   // val lines = sc.textFile("Sample.txt")
    
   // val peopleDFCsv = sc.read.format("csv").option("header", "true").load("Sample2.csv")
   val sqlContext = new org.apache.spark.sql.SQLContext(sc)
   val df = sqlContext.read.json("Person.json")

// Displays the content of the DataFrame to stdout
    df.show()
  }
  
}