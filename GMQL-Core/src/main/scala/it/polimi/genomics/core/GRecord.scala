package it.polimi.genomics.core


/**
 * Created by Abdulrahman Kaitoua on 27/05/15.
 * Email: abdulrahman.kaitoua@polimi.it
 *
 */
class GRecord (key:GRecordKey,values:Array[GValue])
  extends Tuple2(key,values)
  with Serializable {

  def this() = this(new GRecordKey(0, "chr", 0, 0, '.'), Array[GValue]())

  override def toString(): String = {
    val reg = key + "\t"
    values match {
      case _: Array[GValue] => return reg + "\t" + values.iterator.mkString("\t");
    }
    reg
  }
}
