package it.polimi.genomics.tooling

import java.io.File

/**
 * Created by pietro on 04/08/15.
 */
object Comparators {

  def compareMeta(flink_path : String, spark_path : String) : Boolean = {

    val flink_meta = read_flink_results(flink_path + "/meta").sorted
    val spark_meta = read_spark_results(spark_path + "/meta").sorted

    flink_meta==spark_meta
  }

  def compareExp(flink_path : String, spark_path : String) : Boolean = {

    val flink_meta = read_flink_results(flink_path + "/exp").sorted
    val spark_meta = read_spark_results(spark_path + "/exp").sorted

//    flink_meta.zip(spark_meta).foreach(x=>if(x._1 != x._2) println(x._1+"///"+x._2,false)else println(x._1+"///"+x._2,true) )
    println (flink_meta.size, spark_meta.size)
    flink_meta==spark_meta
  }


  def read_flink_results(flink_path : String) : List[String] = {

    val f_flink_path = new File(flink_path)

    if (!f_flink_path.isDirectory ) {
      (for (line <- scala.io.Source.fromFile(f_flink_path).getLines()) yield line)
        .toList
    } else {
      var res : List[String] = List.empty
      for (f <- f_flink_path.listFiles().filter(x => !x.getName.endsWith("schema"))) {
        res = res ++ (for(line <- scala.io.Source.fromFile(f).getLines()) yield line)
      }
      res
    }

  }

  def read_spark_results(spark_path : String) : List[String] = {

    val f_spark_path = new File(spark_path)
    val codec = scala.io.Codec("ISO-8859-1")

    var res : List[String] = List.empty
    for (f <- f_spark_path.listFiles().filter(x => x.getName.startsWith("part-"))) {
        res = res ++ (for(line <- scala.io.Source.fromFile(f)(codec).getLines()) yield line).toList
      }
      res
  }


}
