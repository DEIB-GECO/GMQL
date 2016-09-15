package it.polimi.genomics.flink.FlinkImplementation.reader.parser

import it.polimi.genomics.core.GMQLLoader
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.DataTypes
import it.polimi.genomics.core.{GString, GDouble, GValue}
import it.polimi.genomics.core.exception.ParsingException

/**
 * Parser for general delimiter-separated values files like CSV, or Tab-separated files
 *
 * Created by michelebertoni on 26/04/15.
 *
 * @param delimiter Char used as delimiter i.e. <code>'\t</code>, <code>,</code>, ...
 * @param chrPos Position of the chromosome name in the splitted array
 * @param startPos Position of the starting position of the region in the splitted array
 * @param stopPos Position of the stop position of the region in the splitted array
 * @param strandPos Position of the strand in the splitted array
 * @param otherLongPos Array containing the positions of other Integer or Long values in the splitted array
 * @param otherPos Array containing the positions of other Floating point or Double values in the splitted array
 * @param otherStringPosArray containing the positions of other String values in the splitted array
 */
class DelimiterSeparatedValuesParser (delimiter: Char, chrPos: Int, startPos: Int, stopPos: Int, strandPos: Option[Int], otherPos: Option[Array[(Int, ParsingType.PARSING_TYPE)]]) extends GMQLLoader[(Long,String), DataTypes.FlinkRegionType, (Long,String), DataTypes.FlinkMetaType] with java.io.Serializable {

  var parsingType = "tab"
  final val GTF = "gtf"
  final val VCF = "vcf"
  final val spaceDelimiter = " "
  final val semiCommaDelimiter = ";"

  private val s = new StringBuilder()

  override def meta_parser(t : (Long, String)) : DataTypes.FlinkMetaType = {
    //val s = split(t._2)
    val s = t._2 split delimiter
    try{
      //println(t._2 + " - " + s.mkString("§") + " - " + (t._1, s(0), s(1)))
      (t._1, s(0), s(1))
    } catch {
      case e : Throwable => throw ParsingException.create(t._2, e)
    }
  }

  override def region_parser(t : (Long, String)) : DataTypes.FlinkRegionType = {
    //val s = split(t._2)
    val s = t._2 split delimiter
    try {
      val other = parsingType match {
        case GTF => {
          val score = if (!s(5).trim.equals(".")) // When the values is not present it is considered . and we cast it into 0.0
            GDouble(s(5).trim.toDouble)
          else
            GDouble(0.0)

          val restValues = if (otherPos.get.size > 1) {
            var i = 0
            val values = s(8) split semiCommaDelimiter
            otherPos.get.tail.map { b =>
              val attVal = values(i).trim split spaceDelimiter;
              i += 1

              val attValue = attVal(1).trim.substring(1, attVal(1).trim.length - 1)

              val value:GValue =b._2 match {
                case ParsingType.DOUBLE => GDouble(attValue.toDouble)
                case ParsingType.INTEGER => GDouble(attValue.toInt)
                case ParsingType.STRING => GString(attValue.toString)
              }
              value
            }
          } else Array[GValue]()

          score +: restValues
        }
        case _ => {
          if (otherPos.isDefined) otherPos.get.foldLeft(new Array[GValue](0))((a, b) => a :+ {
            b._2 match {
              case ParsingType.DOUBLE => GDouble(s(b._1).trim.toDouble)
              case ParsingType.STRING => GString(s(b._1).trim.toString)
            }
          })
          else new Array[GValue](0)
        }
      }
//      val other = if (otherPos.isDefined) otherPos.get.foldLeft(new Array[GValue](0))((a, b) => a :+ {
//        b._2 match {
//          //case ParsingType.INTEGER => GInt(s(b._1).toInt)
//          case ParsingType.DOUBLE => GDouble(s(b._1).toDouble)
//          case ParsingType.STRING => GString(s(b._1).toString)
//        }
//      }) else new Array[GValue](0)

      (t._1, s(chrPos), s(startPos).toLong, s(stopPos).toLong,
        if(strandPos.isDefined){
          val c = s(strandPos.get).charAt(0)
          if(c.equals('+') || c.equals('-') || c.equals('*')){
            c
          } else {
            '*'
          }
        } else {
          '*'
        }, other)
    } catch {
      case e : Throwable => throw ParsingException.create(t._2, e)
    }
  }

  /*
  private def split(line : String) : Array[String] = {
    val len = line.length();
    var res = new Array[String](0)

    for (j <- 0 to len-1) {
      if (!line.charAt(j).equals(delimiter)) {
        s.append(line.charAt(j));
      } else {
        res = res :+ s.toString()
        s.setLength(0);
      }
    }

    res
  }
  */

}
