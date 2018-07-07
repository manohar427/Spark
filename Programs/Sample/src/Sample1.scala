import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Sample1 {
  def main(args: Array[String]) {
    val sc1 = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]");
    val sc = new SparkContext(sc1);

    /*  var intArr = Array(11111113, 2, 3, 4, 5, 6, 7, 8, 9, 0);
    var intRDD = sc.parallelize(intArr,10);
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>1");
    var fele = intRDD.first();
    var psize = intRDD.partitions.size;
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>2:" + fele);
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>3:" + psize);
    intRDD.take(3).foreach(println);
    intRDD.collect().foreach(println);*/
    var textRDD = sc.textFile("Sample2.tsv")

    /* var fl = textRDD.first();
    println(fl);
    textRDD.take(2).foreach(println);
    var msrdd = textRDD.map(line => line.split(" "));
    msrdd.collect().foreach(x => {
      println("Map New Line Start=============");
      x.foreach(println)
      println("Map New Line End=============");
    });*/

    //Hash map iteration 
    var fmsrdd = textRDD.flatMap(line => line.split(" "));
    var msrdd = fmsrdd.map(w => (w, 1));
    val counts = msrdd.reduceByKey((a, b) => a + b).sortByKey().collect();
    counts.foreach(x => println(x._1 + "-->" + x._2))
    //for ((k,v) <- counts) println("key:"+k+",Value:"+v)

  }
}