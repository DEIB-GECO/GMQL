package it.polimi.genomics.flink.FlinkImplementation.writer

import it.polimi.genomics.core.DataTypes
import org.apache.flink.api.common.io.FileOutputFormat

/**
 * Created by michelebertoni on 05/05/15.
 */
class DefaultRegionWriter extends FileOutputFormat[DataTypes.FlinkRegionType] {

  private final val buffer : StringBuilder = new StringBuilder()
  private final val separator = "\t"
  private final val terminator = "\n"

  /**
   * Takes a GMQL region and write it into a tab separated file
   * @param record the region to be written
   */
  override def writeRecord(record: DataTypes.FlinkRegionType): Unit = {

    this.buffer setLength 0
    this.buffer append record._1.toString append separator
    this.buffer append record._2 append separator
    this.buffer append record._3.toString append separator
    this.buffer append record._4.toString append separator
    this.buffer append record._5.toString

    for (x <- record._6.iterator) this.buffer append separator append x.toString

    this.buffer append terminator


    val bytes = this.buffer.toString.getBytes



    this.stream write bytes


  }

}