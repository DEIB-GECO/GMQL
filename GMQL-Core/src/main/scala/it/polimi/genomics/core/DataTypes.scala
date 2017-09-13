package it.polimi.genomics.core

import java.math.RoundingMode
import java.text.{DecimalFormatSymbols, DecimalFormat}
import java.util.Locale

//import it.polimi.genomics.core.DataTypes.{GNull, GInt, GString, GDouble}


/**
 * Created by pietro on 10/03/15.
 */
object DataTypes {

  /**
   * SampleID, Chromosome, Start, Stop, Strand, Array of values
   */

  type FlinkRegionType = (Long, String, Long, Long, Char, Array[GValue])

  /**
   * SampleID, AttributeName, AttributeValue
   */
  type FlinkMetaType = (Long, String, String)

  /**
   * Used for grouping in GenometricMap
   * ReferenceSampleID, ExperimentSampleID
   */
  type FlinkMetaJoinType = (Long, Long, Boolean, Boolean)


  /**
   * Used for grouping in GenometricMap
   * ReferenceSampleID, ExperimentSampleID
   */
  type FlinkMetaJoinType3 = (Long, Long)

  /**
   * Used for grouping in GenomietricCover
   * SampleID, List[GroupID]
   */
  type FlinkMetaGroupType = (Long, List[Long])

  /**
   * Used for grouping in GenomietricCover
   * SampleID, GroupID
   */
  type FlinkMetaGroupType2 = (Long, Long)


  /**
   * deprecated
   */
  type FlinkMetaJoinType2 = (Long, List[Long])

  /**
   * Data Type used for reagon operations in Spark
   * We use Key / Value
   * where the Key is the GRecordKey and the value is an array of values
   */
  type GRECORD = (GRecordKey,Array[GValue])

  type SparkMetaJoinType = (Long, Array[Long])
  /**
   * Work with the meta data as a Key /Value for Spark implementation
   * Where the Key is the ID and thwe Value is the Att/Value pair
   */
  type MetaType = (Long,(String, String))
//  case class MetaType(ID:Long, Att:String, Value:String) extends Tuple3(ID,Att,Value)



}

@SerialVersionUID(2212l)
sealed trait GValue extends Serializable /*with Comparable[GValue]*/ with Ordered[GValue]{
  def compare(o : GValue) : Int = {
    this match {
      case GDouble(f) =>   o match {
        case GDouble(v) =>  f compare v
        case GString(v) => 1
        case GInt(v) => f compare v.toDouble
        case GNull() => 0
      }
      case GString(f) =>  o match {
        case GDouble(v) =>  -1
        case GString(v) => f compare v
        case GInt(v) => -1
        case GNull() => 0
      }
      case GInt(f) =>  o match {
        case GDouble(v) =>  f.toDouble compare v
        case GString(v) => 1
        case GInt(v) => f compare v
        case GNull() => 0
      }
      case GNull() => 0
    }
  }
//  def equal(o : GValue) : Boolean = {
//    o match {
//      case GInt(value) => try{value.equals(o.asInstanceOf[GInt].v)} catch { case e : Throwable => false }
//      case GDouble(value) => try{value.equals(o.asInstanceOf[GDouble].v)} catch { case e : Throwable => false }
//      case GString(value) => try{value.equals(o.asInstanceOf[GString].v)} catch { case e : Throwable => false }
//      case GNull() => o.isInstanceOf[GNull]
//      case _ => false
//    }
//  }
//  override def compareTo(o: GValue): Int = {
//    o match {
//      case GInt(value) => try{value.compareTo(o.asInstanceOf[GInt].v)} catch { case e : Throwable => 0 }
//      case GDouble(value) => try{value.compareTo(o.asInstanceOf[GDouble].v)} catch { case e : Throwable => 0 }
//      case GString(value) => try{value.compareTo(o.asInstanceOf[GString].v)} catch { case e : Throwable => 0 }
//      case GNull() => 0
//      case _ => 0
//    }
//  }
}

/**
  * Represents a @GValue that contains an integer
  * @deprecated
  * @param v
  */
@SerialVersionUID(2213l)
case class GInt(v: Int) extends GValue{

  def this() = this(0)
  override def toString() : String = {
    v.toString
  }
  override def equals(other : Any) : Boolean = {
    other match {
      case GInt(value) => value.equals(v)
      case _ => false
    }
  }

}

/**
  * Represents a @GValue that contains a number as a @Double
  * @param v number
  */
@SerialVersionUID(2214l)
case class GDouble(v: Double) extends GValue {//with Ordered[GDouble]{
//    def compare(o: GDouble): Int = {
//      o.v compare this.v
//    }
def this() = this(0.0)
  override def toString() : String = {
    //TODO find a better way for rounding: this is FAST, 100x faster than formatter and bigdecimal but can give errors
    // (Math.round(v * 1000000000000L) * 0.000000000001).toString // <-- NOT WORKING!!! second multiplication get mad
    // (Math.round(v * 1000000000000L).toDouble / 1000000000000D ).toString // working quite good and fast

    val dfs = new DecimalFormatSymbols(Locale.ENGLISH);
    val df = new DecimalFormat("#.########", dfs);
    df.setRoundingMode(RoundingMode.FLOOR);
    df.format(v)

    //v.toString // ROUNDING ERROR
  }
  override def equals(other : Any) : Boolean = {
    other match {
      case GDouble(value) => value.equals(v)
      case _ => false
    }
  }

}

/**
  * Represents a @GValue that contains a @String
  * @param v string
  */
@SerialVersionUID(2215l)
case class GString(v: String) extends GValue{
  def this() = this(".")
  override def toString() : String = {
    v.toString
  }
  override def equals(other : Any) : Boolean = {
    other match {
      case GString(value) => value.equals(v)
      case _ => false
    }
  }
}

@SerialVersionUID(2216l)
case class GNull() extends GValue{
  override def toString() : String = {
    "null"
  }

  override def equals(other : Any) : Boolean = {
    other match {
      case GNull() => true
      case _ => false
    }
  }
}
