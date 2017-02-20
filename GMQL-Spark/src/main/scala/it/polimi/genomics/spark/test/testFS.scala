package it.polimi.genomics.spark.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by abdulrahman on 25/01/2017.
  */
object testFS {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration();
            val path = new org.apache.hadoop.fs.Path("hdfs://localhost:54310/user/akaitoua/");
            val fs = FileSystem.get(path.toUri(), conf);

    println(fs.exists(path))
    println(path.getName)
    println(fs.getConf.toString)
    println (path.getParent.getName)
    println(new java.io.File("/user/akaitoua/").getPath)
  }
}
