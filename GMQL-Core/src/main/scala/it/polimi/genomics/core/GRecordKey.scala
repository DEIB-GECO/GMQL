package it.polimi.genomics.core

/**
 * Created by Abdulrahman Kaitoua on 27/05/15.
 * Email: abdulrahman.kaitoua@polimi.it
 *
 */
class GRecordKey (val id:Long, val chrom:String,val start:Long,val stop:Long,val strand:Char)
  extends Tuple5(id,chrom,start,stop,strand)
  with Ordered[GRecordKey]
  with Serializable
{
  def this()= this(0,"chr",0,0,'.')

  def compare(o: GRecordKey): Int = {
      if (this.id == o.id)
       if (this.chrom.equals(o.chrom))
        if (this.start == o.start)
          if (this.stop == o.stop)
            this.strand compare o.strand
          else this.stop compare o.stop
        else this.start compare o.start
      else this.chrom compare o.chrom
    else this.id compare o.id
  }

  override def toString ():String ={
    val reg = id + "\t"+ chrom + "\t" + start + "\t" + stop+"\t"+strand;
    //    values match {
    //      case _: Array[Any] => return reg+"\t"+values.iterator.mkString("\t");
    //    }
    reg
  }}
