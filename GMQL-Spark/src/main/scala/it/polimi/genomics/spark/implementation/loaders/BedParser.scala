package it.polimi.genomics.spark.implementation.loaders

import java.io.InputStream

import it.polimi.genomics.core._
import it.polimi.genomics.spark.utilities.FSConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML

/**
  * Created by abdulrahman kaitoua on 25/05/15.
  */
/**
  * GMQL Bed Parser, it is a parser that parse delimited text files,
  *
  * @param delimiter [[String]] of the delimiter used to separate columns, can be TAB, space, special Char, or something else.
  * @param chrPos [[Int]] of the position index of chromosome column in the delimited file, this is compulsory column.
  * @param startPos [[Int]] of the position index of region start column in the delimited file, this is compulsory column.
  * @param stopPos [[Int]] of the position index of region stop column in the delimited file, this is compulsory column.
  * @param strandPos [[Int]] of the position index of strand column in the delimited file, this is compulsory column.
  * @param otherPos [[Array]] of the other columns positions, this is [[Option]] and can be [[None]]. The Array has tuple of (position as [[Int]],[[ParsingType]])
  */
class BedParser(delimiter: String, var chrPos: Int, var startPos: Int, var stopPos: Int, var strandPos: Option[Int], var otherPos: Option[Array[(Int, ParsingType.PARSING_TYPE)]]) extends GMQLLoader[(Long, String), Option[DataTypes.GRECORD], (Long, String), Option[DataTypes.MetaType]] with java.io.Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[BedParser]);
  var parsingType = GMQLSchemaFormat.TAB
  var coordinateSystem = GMQLSchemaCoordinateSystem.ZeroBased
  final val spaceDelimiter: String = " "
  final val semiCommaDelimiter: String = ";"

  /**
    * Meta Data Parser to parse String to GMQL META TYPE (ATT, VALUE)
    *
    * @param t
    * @return
    */
  override def meta_parser(t: (Long, String)): Option[DataTypes.MetaType] = {
    try {
      val s = t._2 split delimiter
      Some((t._1, (s(0), s(1))))
    } catch {
      case ex: ArrayIndexOutOfBoundsException => logger.warn("could not cast this line: \n\t\t" + t); None
    }
  }

  /**
    * Parser of String to GMQL Spark GRECORD
    *
    * @param t [[Tuple2]] of the ID and the [[String]] line to be parsed.
    * @return [[GRecord]] as GMQL record representation.
    */
  override def region_parser(t: (Long, String)): Option[DataTypes.GRECORD] = {

    try {

      val s: Array[String] = t._2 split delimiter

      val other = parsingType match {
        case GMQLSchemaFormat.GTF => {
          val score = if (!s(5).trim.equals(".") && !s(5).trim.toLowerCase().equals("null")) // When the values is not present it is considered . and we cast it into 0.0
            GDouble(s(5).trim.toDouble)
          else
            GNull()

          val source = GString(s(1).trim)
          val feature = GString(s(2).trim)
          val frame = GString(s(7).trim)

          val restValues = if (otherPos.get.size > 1) {
            var i = 0
            val values = s(8) split semiCommaDelimiter
            otherPos.get.tail.tail.tail.tail.map { b =>
              val attVal = values(i).trim split spaceDelimiter;
              i += 1

              val attValue = attVal(1).trim.substring(1, attVal(1).trim.length - 1)

              val value:GValue =b._2 match {
                case ParsingType.DOUBLE => if(attValue.toLowerCase().equals("null")) {
                  /*GDouble(0.0)*/ GNull()
                } else GDouble(attValue.toDouble)
                case ParsingType.INTEGER => if(attValue.toLowerCase().equals("null")) {
                  /*GDouble(0)*/ GNull()
                } else GDouble(attValue.toInt)
                case ParsingType.STRING => GString(attValue.toString)
              }
              value
            }
          } else Array[GValue]()

          Array(source,feature,score,frame )++ restValues
        }
        case _ => {
          if (otherPos.isDefined) otherPos.get.foldLeft(new Array[GValue](0))((a, b) => a :+ {
          b._2 match {
            case ParsingType.DOUBLE => if(s(b._1).trim.toLowerCase().equals("null") || s(b._1).trim.equals(".")) {
              GNull()
            } else GDouble(s(b._1).trim.toDouble)
            case ParsingType.STRING => GString(s(b._1).trim)
          }
        })
        else new Array[GValue](0)
      }
      }

      val newStart = if (coordinateSystem == GMQLSchemaCoordinateSystem.OneBased) s(startPos).trim.toLong - 1 else s(startPos).trim.toLong

      Some((new GRecordKey(t._1, s(chrPos).trim, newStart, s(stopPos).trim.toLong,
        if (strandPos.isDefined) {
          val c = s(strandPos.get).trim.charAt(0)
          //          if(c != '*') println(t._2+"\t"+c+"\n"+s.mkString("\t"))
          if (c.equals('+') || c.equals('-')) {
            c
          } else {
            '*'
          }
        } else {
          '*'
        }

      ), other))
    } catch {
      case e: Throwable =>
        logger.warn(e.getMessage)
        logger.warn("Chrom: " + chrPos + "\tStart: " + startPos + "\tStop: " + stopPos + "\tstrand: " + strandPos);
        logger.warn("Values: " + otherPos.getOrElse(Array[(Int, ParsingType.PARSING_TYPE)]()).map(x => "(" + x._1 + "," + x._2 + ")").mkString("\t") + "\n" +
          "This line can not be casted (check the spacing): \n\t\t" + t);
        None
    }
  }

  //
}

/**
  *
  * Standard Full BED Parser of 10 Columns
  *
  */
object BedParser extends BedParser("\t", 0, 1, 2, Some(5), Some(Array((3, ParsingType.STRING), (4, ParsingType.DOUBLE), (6, ParsingType.DOUBLE), (7, ParsingType.DOUBLE), (8, ParsingType.DOUBLE), (9, ParsingType.DOUBLE)))) {
  schema = List(("3", ParsingType.DOUBLE), ("4", ParsingType.DOUBLE), ("6", ParsingType.DOUBLE), ("7", ParsingType.DOUBLE), ("8", ParsingType.DOUBLE), ("9", ParsingType.DOUBLE))
}

/**
  *
  * Test parser, should be removed
  *
  * @deprecated
  */
object testParser {
  def apply(): BedParser = {
    new BedParser("\t", 0, 1, 2, Some(5), Some(Array((3, ParsingType.STRING), (4, ParsingType.DOUBLE), (6, ParsingType.STRING), (7, ParsingType.STRING), (8, ParsingType.DOUBLE))))
  }
}

/**
  *
  * Test parser, should be removed
  *
  * @deprecated
  */
object test1Parser {
  def apply(): BedParser = {
    new BedParser("\t", 0, 1, 2, Some(3), Some(Array((4, ParsingType.STRING))))
  }
}

/**
  *
  * Test parser, should be removed
  *
  * @deprecated
  */
object test2Parser {
  def apply(): BedParser = {
    new BedParser("\t", 0, 1, 2, Some(3), Some(Array((4, ParsingType.DOUBLE))))
  }
}

/**
  *
  * Annotation Parser, 6 columns
  *
  */
object ANNParser extends BedParser("\t", 0, 1, 2, Some(5), Some(Array((3, ParsingType.STRING), (4, ParsingType.DOUBLE)))) {
  schema = List(("name", ParsingType.STRING), ("score", ParsingType.DOUBLE))
}


object BroadProjParser extends BedParser("\t", 0, 1, 2, None, Some(Array((3, ParsingType.STRING)))){

  schema = List(("name", ParsingType.STRING))
}

/**
  *
  * Parser for Chr, Start, Stop only (no Strand)
  *
  */
object BasicParser extends BedParser("\t", 0, 1, 2, None, None){

  schema = List()
}

/**
  *
  * Test parser, should be removed
  *
  * @deprecated
  */
object BroadPeaksParser extends BedParser("\t", 0, 1, 2, Some(5), Some(Array((3, ParsingType.STRING), (4, ParsingType.DOUBLE), (6, ParsingType.DOUBLE), (7, ParsingType.DOUBLE), (8, ParsingType.DOUBLE)))) {
  schema = List(("name", ParsingType.STRING), ("score", ParsingType.DOUBLE), ("signal", ParsingType.DOUBLE), ("pvalue", ParsingType.DOUBLE), ("qvalue", ParsingType.DOUBLE))
}

/**
  *
  * Narrow Peaks Parser 10 columns
  *
  *
  */
object NarrowPeakParser extends BedParser("\t", 0, 1, 2,
  Some(5),
  Some(Array((3, ParsingType.STRING), (4, ParsingType.DOUBLE), (6, ParsingType.DOUBLE), (7, ParsingType.DOUBLE), (8, ParsingType.DOUBLE), (9, ParsingType.DOUBLE)))) {

  schema = List(("name", ParsingType.STRING),
    ("score", ParsingType.DOUBLE),
    ("signalValue", ParsingType.DOUBLE),
    ("pValue", ParsingType.DOUBLE),
    ("qValue", ParsingType.DOUBLE),
    ("peak", ParsingType.DOUBLE))

  override def region_parser(t: (Long, String)): Option[(GRecordKey, Array[GValue])] = {

    val s: Array[String] = t._2 split "\t"

    val name = GString(s(3).trim)
    val score_raw = s(4).trim.toLowerCase
    val score = if (score_raw.equals(".") || score_raw.equals("null")) GNull() else GDouble(score_raw.toDouble)
    val sigVal_raw = s(6).trim.toLowerCase
    val sigVal = if (sigVal_raw.equals(".") || sigVal_raw.equals("null")) GNull() else GDouble(sigVal_raw.toDouble)
    val pVal_raw = s(7).trim.toLowerCase
    val pVal = if (pVal_raw.equals(".") || pVal_raw.equals("null")) GNull() else GDouble(pVal_raw.toDouble)
    val qVal_raw = s(8).trim.toLowerCase
    val qVal = if (qVal_raw.equals(".") || qVal_raw.equals("null")) GNull() else GDouble(qVal_raw.toDouble)
    val peak_raw = s(9).trim.toLowerCase
    val peak = if (peak_raw.equals(".") || peak_raw.equals("null")) GNull() else GDouble(peak_raw.toDouble)

    val other = Array[GValue](name, score, sigVal, pVal, qVal, peak)

    val newStart = s(startPos).trim.toLong

    val c = s(strandPos.get).trim.charAt(0)
    val strand = if (c.equals('+') || c.equals('-')) {
      c
    } else {
      '*'
    }

    Some(
      (new GRecordKey(
        t._1,
        s(chrPos).trim,
        newStart,
        s(stopPos).trim.toLong,
        strand),
        other))
  }
}

/**
  * @deprecated
  */
object testOrder extends BedParser("\t", 0, 1, 2, Some(3), Some(Array((4, ParsingType.STRING), (5, ParsingType.DOUBLE), (6, ParsingType.DOUBLE), (7, ParsingType.DOUBLE), (8, ParsingType.DOUBLE), (9, ParsingType.DOUBLE)))) {

  schema = List(("name", ParsingType.STRING),
    ("score", ParsingType.DOUBLE),
    ("signalValue", ParsingType.DOUBLE),
    ("pValue", ParsingType.DOUBLE),
    ("qValue", ParsingType.DOUBLE),
    ("peak", ParsingType.DOUBLE))
}
/**
  *
  * Test parser, should be removed
  *
  * @deprecated
  */
object test3Parser {
  def apply(): BedParser = {
    new BedParser("\t", 0, 1, 2, Some(5), Some(Array((3, ParsingType.STRING), (4, ParsingType.DOUBLE))))
  }
}


/**
  *
  * Test parser, should be removed
  *
  * @deprecated
  */
object H19BedAnnotationParser extends BedParser("\t", 0, 1, 2, Some(5), Some(Array((3, ParsingType.DOUBLE), (4, ParsingType.DOUBLE)))) {
  schema = List(("name", ParsingType.DOUBLE), ("score", ParsingType.DOUBLE))
}

/**
  *
  * RNA Seq Parser
  *
  */
object RnaSeqParser extends BedParser("\t", 0, 1, 2, Some(3), Some(Array((4, ParsingType.STRING), (5, ParsingType.DOUBLE)))) {
  schema = List(("name", ParsingType.STRING), ("score", ParsingType.DOUBLE))
}

/**
  * Custom parser that reads the schema xml file and then provide the schema internally for parsing the data using the BED PARSER
  */
class CustomParser extends BedParser("\t", 0, 1, 2, Some(3), Some(Array((4, ParsingType.DOUBLE)))) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[CustomParser]);

  def setSchema(dataset: String): BedParser = {

    val path: Path = new Path(dataset);
    val fs: FileSystem = FileSystem.get(path.toUri(), FSConfig.getConf);

    //todo: remove this hard fix used for remote execution
    val XMLfile:InputStream = fs.open(new Path(dataset+ (if(!dataset.endsWith("schema"))"/test.schema" else "")))
    var schematype = GMQLSchemaFormat.TAB
    var coordinatesystem = GMQLSchemaCoordinateSystem.Default
    var schema: Array[(String, ParsingType.Value)] = null

    try {
      val schemaXML = XML.load(XMLfile);
      val cc = (schemaXML \\ "field")
      schematype = GMQLSchemaFormat.getType((schemaXML \\ "gmqlSchema").head.attribute("type").get.head.text.trim.toLowerCase())
      val coordSysAttr = (schemaXML \\ "gmqlSchema").head.attribute("coordinate_system")
      coordinatesystem = GMQLSchemaCoordinateSystem.getType(if (coordSysAttr.isDefined) coordSysAttr.get.head.text.trim.toLowerCase() else "default")
      schema = cc.map(x => (x.text.trim, ParsingType.attType(x.attribute("type").get.head.text))).toArray
    } catch {
      case x: Throwable => x.printStackTrace(); logger.error(x.getMessage); throw new RuntimeException(x.getMessage)
    }

    coordinatesystem match {
      case GMQLSchemaCoordinateSystem.ZeroBased => coordinateSystem = GMQLSchemaCoordinateSystem.ZeroBased
      case GMQLSchemaCoordinateSystem.OneBased => coordinateSystem = GMQLSchemaCoordinateSystem.OneBased
      case _ => coordinateSystem = GMQLSchemaCoordinateSystem.Default
    }

    schematype match {
      case  GMQLSchemaFormat.VCF =>{
        parsingType = GMQLSchemaFormat.VCF

        if (coordinateSystem == GMQLSchemaCoordinateSystem.Default) coordinateSystem = GMQLSchemaCoordinateSystem.OneBased

        val valuesPositions = schema.zipWithIndex.flatMap { x => val name = x._1._1;
          if (checkCoordinatesName(name)) None
          else Some(x._2+2, x._1._2)
        }

        val valuesPositionsSchema = schema.flatMap { x => val name = x._1;
          if ( checkCoordinatesName(name)) None
          else Some(x._1, x._2)
        }.toList

        val other = if (valuesPositions.length > 0)
          (5, ParsingType.DOUBLE) +: valuesPositions
        else
          Array((5, ParsingType.DOUBLE))

        chrPos = 0
        startPos = 1
        stopPos = 1
        strandPos = None
        otherPos = Some(other)

        this.schema = valuesPositionsSchema
      }
      case GMQLSchemaFormat.GTF => {
        parsingType = GMQLSchemaFormat.GTF

        if (coordinateSystem == GMQLSchemaCoordinateSystem.Default) coordinateSystem = GMQLSchemaCoordinateSystem.OneBased

        val valuesPositions: Array[(Int, ParsingType.Value)] = schema.flatMap { x => val name = x._1.toUpperCase();
          if (name.equals("SEQNAME") || name.equals("SOURCE") ||name.equals("FEATURE") ||name.equals("FRAME") ||name.equals("SCORE") || checkCoordinatesName(name)) None
          else Some(8, x._2)
        }

        val valuesPositionsSchema = schema.flatMap { x => val name = x._1.toUpperCase();
          if (name.equals("SEQNAME") ||name.equals("SOURCE") ||name.equals("FEATURE") ||name.equals("FRAME") ||name.equals("SCORE") || checkCoordinatesName(name)) None
          else Some(x._1, x._2)
        }.toList

        val other: Array[(Int, ParsingType.Value)] = if (valuesPositions.length > 0)
          Array[(Int, ParsingType.Value)]((1, ParsingType.STRING) , (2, ParsingType.STRING), (5, ParsingType.DOUBLE), (7, ParsingType.STRING)) ++ valuesPositions
        else
          Array((5, ParsingType.DOUBLE))

        chrPos = 0;
        startPos = 3;
        stopPos = 4;
        strandPos = Some(6);
        otherPos = Some(other)

        this.schema = List(("source", ParsingType.STRING),("feature", ParsingType.STRING),("score", ParsingType.DOUBLE),("frame", ParsingType.STRING)) ++ valuesPositionsSchema
      }

      case _ => {

        if (coordinateSystem == GMQLSchemaCoordinateSystem.Default) coordinateSystem = GMQLSchemaCoordinateSystem.ZeroBased

        val schemaWithIndex = schema.zipWithIndex
        val chrom = schemaWithIndex.filter(x => (x._1._1.toUpperCase().equals("CHROM") || x._1._1.toUpperCase().equals("CHROMOSOME") || x._1._1.toUpperCase().equals("CHR")))
        val start = schemaWithIndex.filter(x => (x._1._1.toUpperCase().equals("START") || x._1._1.toUpperCase().equals("LEFT")))
        val stop = schemaWithIndex.filter(x => (x._1._1.toUpperCase().equals("STOP") || x._1._1.toUpperCase().equals("RIGHT") || x._1._1.toUpperCase().equals("END")))
        val strand = schemaWithIndex.filter(x => (x._1._1.toUpperCase().equals("STR") || x._1._1.toUpperCase().equals("STRAND")))

        var missing = 0
        val chromPosition = if (chrom.size > 0) chrom.head._2
        else {
          missing += 1; 0
        }
        val startPosition = if (start.size > 0) start.head._2
        else {
          missing += 1; 1
        }
        val stopPosition = if (stop.size > 0) stop.head._2
        else {
          missing += 1; 2
        }
        val strPosition = if (strand.size > 0) Some(strand.head._2 + missing)
        else {
          logger.warn("Strand is not specified in the XML schema file, the default strand (which is * is selected.")
          None
        } //in this case strand considered not present

        val valuesPositions = schemaWithIndex.flatMap { x => val name = x._1._1;
          if (checkCoordinatesName(name)) None
          else Some(x._2 + missing, x._1._2)
        }
        val valuesPositionsSchema = schemaWithIndex.flatMap { x => val name = x._1._1;
          if ( checkCoordinatesName(name)) None
          else Some(x._1)
        }
        chrPos = chromPosition;
        startPos = startPosition;
        stopPos = stopPosition;
        strandPos = strPosition;
        otherPos = Some(valuesPositions)


        this.schema = valuesPositionsSchema.toList
      }
    }

    this

  }

  /**
    * check the name of the coordinates in the schema xml file.
    *
    * @param fieldName the column name in the schemas
    * @return return True when the column is a coordinate column, otherwise false.
    */
  def checkCoordinatesName(fieldName:String):Boolean={
    (fieldName.toUpperCase().equals("CHROM") || fieldName.toUpperCase().equals("CHROMOSOME") ||
      fieldName.toUpperCase().equals("CHR") || fieldName.toUpperCase().equals("START") ||
      fieldName.toUpperCase().equals("STOP") || fieldName.toUpperCase().equals("LEFT") ||
      fieldName.toUpperCase().equals("RIGHT") || fieldName.toUpperCase().equals("END") ||
      fieldName.toUpperCase().equals("STRAND") || fieldName.toUpperCase().equals("STR"))
  }
}


object BedScoreParser extends BedParser("\t", 0, 1, 2, None, Some(Array((3, ParsingType.DOUBLE)))) {
  schema = List(("score", ParsingType.DOUBLE))
}