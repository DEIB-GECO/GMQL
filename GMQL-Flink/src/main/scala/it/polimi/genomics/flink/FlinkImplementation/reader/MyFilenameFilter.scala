package it.polimi.genomics.flink.FlinkImplementation.reader

import java.io.{File, FilenameFilter}

/**
 * Created by michelebertoni on 27/07/15.
 */
abstract class MyFilenameFilter() extends FilenameFilter with Serializable {
}

object DataSetFilter extends  MyFilenameFilter{
  override def accept(dir : File, name : String) : Boolean = {
    val lowercaseName = name.toLowerCase()
    //TODO
    if (!new File(dir + "/" + name).isDirectory && !lowercaseName.endsWith(".meta") && !lowercaseName.startsWith(".")) {
      true
    } else {
      false
    }
  }
}
