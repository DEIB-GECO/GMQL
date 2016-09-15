package it.polimi.genomics.core

/**
 * Created by Abdulrahman Kaitoua on 27/05/15.
 * Email: abdulrahman.kaitoua@polimi.it
 *
 */
class GRecordKey (id:Long, chrom:String,start:Long,stop:Long,strand:Char)
  extends Tuple5(id,chrom,start,stop,strand)
  with Ordered[GRecordKey]
  with Serializable
{
  def this()= this(0,"chr",0,0,'.')

  def compare(o: GRecordKey): Int = {
    if (this.id.equals(o._1))
      if (this.chrom.equals(o._2))
        if (this.start == o._3)
          if (this.stop == o._4)
            this.stop compare o._5
          else this.stop compare o._4
        else this.start compare o._3
      else this.chrom compare o._2
    else this.id compare o._1
  }

  override def toString ():String ={
    val reg = id + "\t"+ chrom + "\t" + start + "\t" + stop+"\t"+strand;
    //    values match {
    //      case _: Array[Any] => return reg+"\t"+values.iterator.mkString("\t");
    //    }
    reg
  }}
