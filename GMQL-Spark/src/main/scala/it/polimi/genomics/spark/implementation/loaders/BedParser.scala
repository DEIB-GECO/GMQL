package it.polimi.genomics.spark.implementation.loaders

import java.io.{InputStream, FileInputStream, File}

import it.polimi.genomics.core.GMQLLoader
import it.polimi.genomics.core.{GValue, GString, GDouble}
import it.polimi.genomics.core.{DataTypes, GRecordKey, ParsingType}
import it.polimi.genomics.repository.util.Utilities
import org.slf4j.LoggerFactory
import scala.xml.XML

/**
  * Created by abdulrahman kaitoua on 25/05/15.
  */
class BedParser(delimiter: String, var chrPos: Int, var startPos: Int, var stopPos: Int, var strandPos: Option[Int], var otherPos: Option[Array[(Int, ParsingType.PARSING_TYPE)]]) extends GMQLLoader[(Long, String), Option[DataTypes.GRECORD], (Long, String), Option[DataTypes.MetaType]] with java.io.Serializable {

  private val logger = LoggerFactory.getLogger(classOf[BedParser]);
  var parsingType = "tab"
  final val GTF = "gtf"
  final val VCF = "vcf"
  final val spaceDelimiter = " "
  final val semiCommaDelimiter = ";"

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
    * @param t
    * @return
    */
  override def region_parser(t: (Long, String)): Option[DataTypes.GRECORD] = {

    try {

      val s = t._2 split delimiter

      val other = parsingType match {
        case GTF => {
          val score = if (!s(5).trim.equals(".") && !s(5).trim.equals("null")) // When the values is not present it is considered . and we cast it into 0.0
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
                case ParsingType.DOUBLE => if(attValue.equals("null")) {
                  GDouble(0.0)
                } else GDouble(attValue.toDouble)
                case ParsingType.INTEGER => if(attValue.equals("null")) {
                  GDouble(0)
                } else GDouble(attValue.toInt)
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

      Some((new GRecordKey(t._1, s(chrPos).trim, s(startPos).trim.toLong, s(stopPos).trim.toLong,
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
        None //throw ParsingException.create(t._2, e)
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
object NarrowPeakParser extends BedParser("\t", 0, 1, 2, Some(5), Some(Array((3, ParsingType.STRING), (4, ParsingType.DOUBLE), (6, ParsingType.DOUBLE), (7, ParsingType.DOUBLE), (8, ParsingType.DOUBLE), (9, ParsingType.DOUBLE)))) {

  schema = List(("name", ParsingType.STRING),
    ("score", ParsingType.DOUBLE),
    ("signalValue", ParsingType.DOUBLE),
    ("pValue", ParsingType.DOUBLE),
    ("qValue", ParsingType.DOUBLE),
    ("peak", ParsingType.DOUBLE))
}

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

  private val logger = LoggerFactory.getLogger(classOf[CustomParser]);
  //val schema = List(("name", ParsingType.STRING), ("score", ParsingType.DOUBLE))
  def setSchema(dataset: String, username :String = Utilities.USERNAME): BedParser = {

    val hdfs = Utilities.getInstance().getFileSystem
    val XMLfile:InputStream = if(dataset.startsWith("hdfs")|| dataset.startsWith(Utilities.getInstance().HDFSRepoDir))
      hdfs.open(new org.apache.hadoop.fs.Path(dataset+"/test.schema"))
//    else if (new File(dataset).exists()) {
//      new FileInputStream(new File(dataset+"test.schema"))
//    }
    else if (dataset.contains("/")) {
      hdfs.open(new org.apache.hadoop.fs.Path(Utilities.getInstance().gethdfsConfiguration().get("fs.defaultFS")
        +Utilities.getInstance().HDFSRepoDir + username + "/regions" +
        dataset+"/test.schema"))
    } else if(!Utilities.getInstance().checkDSNameinPublic(dataset))
        new FileInputStream(new File(Utilities.getInstance().RepoDir+username+"/schema/"+dataset+".schema"))
    else
      new FileInputStream(new File(Utilities.getInstance().RepoDir+"public"+"/schema/"+dataset+".schema"))

//    println ("HI ",dataset,Utilities.getInstance().gethdfsConfiguration().get("fs.defaultFS") +Utilities.getInstance().HDFSRepoDir + Utilities.USERNAME + "/regions" + dataset+"/test.schema")

    var schematype = "tab"
    var schema: Array[(String, ParsingType.Value)] = null

    try {
      val schemaXML = XML.load(XMLfile);
      val cc = (schemaXML \\ "field")
      schematype = (schemaXML \\ "gmqlSchema").head.attribute("type").get.head.text
      schema = cc.map(x => (x.text.trim, attType(x.attribute("type").get.head.text))).toArray
    } catch {
      case x: Throwable => x.printStackTrace(); logger.error(x.getMessage); throw new RuntimeException(x.getMessage)
    }

    schematype.trim.toLowerCase() match {
      case VCF =>{
        parsingType = VCF

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
      case GTF => {
        parsingType = GTF

        val valuesPositions = schema.flatMap { x => val name = x._1.toUpperCase();
          if (name.equals("SEQNAME") || name.equals("SOURCE") ||name.equals("FEATURE") ||name.equals("FRAME") ||name.equals("SCORE") || checkCoordinatesName(name)) None
          else Some(8, x._2)
        }

        val valuesPositionsSchema = schema.flatMap { x => val name = x._1.toUpperCase();
          if (name.equals("SEQNAME") ||name.equals("SOURCE") ||name.equals("FEATURE") ||name.equals("FRAME") ||name.equals("SCORE") || checkCoordinatesName(name)) None
          else Some(x._1, x._2)
        }.toList

        val other = if (valuesPositions.length > 0)
          (5, ParsingType.DOUBLE) +: valuesPositions
        else
          Array((5, ParsingType.DOUBLE))

        chrPos = 0;
        startPos = 3;
        stopPos = 4;
        strandPos = Some(6);
        otherPos = Some(other)

        this.schema = List(("score", ParsingType.DOUBLE)) ++ valuesPositionsSchema
      }

      case _ => {

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





//    println (this.schema.mkString("\t"))

    this

  }


  def checkCoordinatesName(fieldName:String):Boolean={
    (fieldName.toUpperCase().equals("CHROM") || fieldName.toUpperCase().equals("CHROMOSOME") ||
      fieldName.toUpperCase().equals("CHR") || fieldName.toUpperCase().equals("START") ||
      fieldName.toUpperCase().equals("STOP") || fieldName.toUpperCase().equals("LEFT") ||
      fieldName.toUpperCase().equals("RIGHT") || fieldName.toUpperCase().equals("END") ||
      fieldName.toUpperCase().equals("STRAND") || fieldName.toUpperCase().equals("STR"))
  }
  def attType(x: String) = x.toUpperCase match {
    case "STRING" => ParsingType.STRING
    case "CHAR" => ParsingType.STRING
    case "CHARACTAR" => ParsingType.STRING
    case "BOOLEAN" => ParsingType.STRING
    case "BOOL" => ParsingType.STRING
    case _ => ParsingType.DOUBLE
  }
}


object BedScoreParser extends BedParser("\t", 0, 1, 2, None, Some(Array((3, ParsingType.DOUBLE)))) {
  schema = List(("score", ParsingType.DOUBLE))
}