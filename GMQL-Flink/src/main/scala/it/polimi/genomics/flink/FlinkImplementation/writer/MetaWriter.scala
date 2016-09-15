package it.polimi.genomics.flink.FlinkImplementation.writer

import it.polimi.genomics.core.DataTypes
import it.polimi.genomics.core.DataTypes.FlinkMetaType
import org.apache.flink.api.common.io.FileOutputFormat
import org.apache.flink.core.fs.{FSDataOutputStream, Path}
;

/**
 * Created by michelebertoni on 09/09/15.
 */
class MetaWriter extends FileOutputFormat[DataTypes.FlinkMetaType] {

  private var basePath : Path = null
  private var currentID : Option[Long] = None


  private final val buffer : StringBuilder = new StringBuilder()
  private final val separator : String = "\t"
  private final val terminator : String = "\n"


  def this(path : Path) = {
    this()
    setOutputFilePath(path, true)
  }

  override def open(taskNumber : Int, numTasks : Int) = {
    open(taskNumber, numTasks, false)
  }

  def open(taskNumber : Int, numTasks : Int, internalCall : Boolean) = {
    if(internalCall) {
      super.open(taskNumber, numTasks)
    }
  }

  override def setOutputFilePath(path : Path) = {
    setOutputFilePath(path, true)
  }

  def setOutputFilePath(path: Path, base : Boolean) = {
    super.setOutputFilePath(path)
    if(base){
      basePath = path
    }
  }

  override def writeRecord(record: FlinkMetaType) = {
    if(!record._1.equals(currentID.getOrElse(null))){
      setOutputFilePath(basePath.suffix("/" + record._1.toString+".meta"), false)
      open(0, 1, true)
      currentID = Some(record._1)
    }
    writeRecord(record, this.stream)
  }


  def writeRecord(record : FlinkMetaType, stream : FSDataOutputStream) = {
    this.buffer setLength 0
    this.buffer append record._2 append separator
    this.buffer append record._3.toString

    this.buffer append terminator

    val bytes = this.buffer.toString.getBytes

    stream write bytes

  }

}
