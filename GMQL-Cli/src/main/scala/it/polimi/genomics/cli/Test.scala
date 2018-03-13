package it.polimi.genomics.cli

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


object Test {

  def main(args: Array[String]): Unit = {

    val scriptPath = "/Users/andreagulino/Desktop/query.txt"
    val sparkConf  = "/usr/local/spark/conf/spark-defaults.conf"
    val output = "/Users/andreagulino/Desktop/results/exp"

    val args = Array( "-scriptpath" , scriptPath, "-sparkconffile", sparkConf)

    // Delete output
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    fs.delete(new Path(output),true)

    GMQLExecuteCommand.main(args)

  }
}
