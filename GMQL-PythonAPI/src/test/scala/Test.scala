import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by luca on 08/04/17.
  */

object Test {
  def main(args: Array[String]): Unit = {
    println("Ciao a tutti! Questa è una prova SCALA e SPARK")

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("prova_spark")
    val sc = new SparkContext(conf)

  }
}
