import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.functions.col

object DataFrames {
  def main(args: Array[String]) {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val sc1 = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory", "1g");
    val sc = new SparkContext(sc1);
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val df = sqlContext.read.json("Person.json")
  /*  import df.sparkSession.implicits._

    println("Json data........... Start");
    df.show(3)
    println("Json data........... End");
    df.printSchema();
    df.select("name").show()

    df.groupBy("age").count().show()
    df.select($"name", $"age" + 1).show()

    df.filter($"age" > 80).show(1000)*/
    
    df.createOrReplaceTempView("people")
    
    val sqlDF = sqlContext.sql("SELECT age as AGE,name as NAME FROM people")
sqlDF.show()
//df.createGlobalTempView("people")

  }
}