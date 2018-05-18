package it.polimi.genomics.spark.implementation.loaders

import java.io.InputStream

import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.exception.ParsingException
import it.polimi.genomics.core.{GMQLSchemaCoordinateSystem, GMQLSchemaFormat, ParsingType, _}
import it.polimi.genomics.spark.utilities.FSConfig
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML

/**
  * Created by abdulrahman kaitoua on 25/05/15.
  */

/**
  * contains the helper functions for parsers
  */

object BedParserHelper {
  /**
    * returns parsed data as GValue by using parsingType and the value.
    *
    * @param parsingType
    * @param value
    * @return
    */
  def parseRegion(parsingType: ParsingType.Value, value: String): GValue = {
    parsingType match {
      case ParsingType.STRING | ParsingType.CHAR =>
        // TODO: better to differentiate char from string, however all the implementation does not take care of this
        GString(value.trim)
      case ParsingType.INTEGER | ParsingType.LONG | ParsingType.DOUBLE =>
        value.toLowerCase match {
          case "null" | "." =>
            GNull()
          case _ =>
            //No need to trim
            GDouble(value.toDouble)
        }
      case ParsingType.NULL =>
        GNull()
      case _ =>
        throw new Exception("Unknown ParsingType")
    }
  }


}


/**
  * GMQL Bed Parser, it is a parser that parse delimited text files,
  *
  * @param delimiter [[String]] of the delimiter used to separate columns, can be TAB, space, special Char, or something else.
  * @param chrPos    [[Int]] of the position index of chromosome column in the delimited file, this is compulsory column.
  * @param startPos  [[Int]] of the position index of region start column in the delimited file, this is compulsory column.
  * @param stopPos   [[Int]] of the position index of region stop column in the delimited file, this is compulsory column.
  * @param strandPos [[Int]] of the position index of strand column in the delimited file, this is compulsory column.
  * @param otherPos  [[Array]] of the other columns positions, this is [[Option]] and can be [[None]]. The Array has tuple of (position as [[Int]],[[ParsingType]])
  */
class BedParser(delimiter: String, var chrPos: Int, var startPos: Int, var stopPos: Int, var strandPos: Option[Int], var otherPos: Option[Array[(Int, ParsingType.PARSING_TYPE)]]) extends GMQLLoader[(Long, String), Option[DataTypes.GRECORD], (Long, String), Option[DataTypes.MetaType]] with java.io.Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[BedParser])
  var parsingType: GMQLSchemaFormat.Value = GMQLSchemaFormat.TAB
  var coordinateSystem: GMQLSchemaCoordinateSystem.Value = GMQLSchemaCoordinateSystem.ZeroBased
  final val spaceDelimiter: String = " "
  final val semiCommaDelimiter: String = ";"


  @transient
  private lazy val otherGtf: Map[String, (PARSING_TYPE, Int)] =
    schema
      .zipWithIndex
      .map(x => x._1._1.toUpperCase -> (x._1._2, x._2))
      .toMap

  @transient
  private lazy val otherGtfSize: Int = otherGtf.size

  @deprecated
  def calculateMapParameters(namePosition: Option[Seq[String]] = None): Unit = {}

  /**
    * Meta Data Parser to parse String to GMQL META TYPE (ATT, VALUE)
    *
    * @param t
    * @return
    */
  @throws(classOf[ParsingException])
  override def meta_parser(t: (Long, String)): Option[DataTypes.MetaType] = {
    val s = t._2.split(delimiter, -1)

    if (s.length != 2 && s.length != 0 && !s.startsWith("#")) {
      throw ParsingException.create("The following metadata entry is not in the correct format: \n[" + t._2 + "]\nCheck the spacing.")
    } else if (s.length == 0) {
      None
    } else {
      Some((t._1, (s(0), s(1))))
    }
  }

  /**
    * Parser of String to GMQL Spark GRECORD
    *
    * @param t [[Tuple2]] of the ID and the [[String]] line to be parsed.
    * @return [[DataTypes.GRECORD]] as GMQL record representation.
    */
  @throws(classOf[ParsingException])
  override def region_parser(t: (Long, String)): Option[DataTypes.GRECORD] = {
    import BedParserHelper._
    try {
      val s: Array[String] = t._2.split(delimiter, -1)

      val other = parsingType match {
        case GMQLSchemaFormat.GTF =>
          // GTF file format definition
          // 0) seqname - name of the chromosome or scaffold; chromosome names can be given with or without the 'chr' prefix. Important note: the seqname must be one used within Ensembl, i.e. a standard chromosome name or an Ensembl identifier such as a scaffold ID, without any additional content such as species or assembly. See the example GFF output below.
          // 1) source - name of the program that generated this feature, or the data source (database or project name)
          // 2) feature - feature type name, e.g. Gene, Variation, Similarity
          // 3) start - Start position of the feature, with sequence numbering starting at 1.
          // 4) end - End position of the feature, with sequence numbering starting at 1.
          // 5) score - A floating point value.
          // 6) strand - defined as + (forward) or - (reverse).
          // 7) frame - One of '0', '1' or '2'. '0' indicates that the first base of the feature is the first base of a codon, '1' that the second base is the first base of a codon, and so on..
          // 8) attribute - A semicolon-separated list of tag-value pairs, providing additional information about each feature.

          val source = parseRegion(ParsingType.STRING, s(1))
          val feature = parseRegion(ParsingType.STRING, s(2))
          val score = parseRegion(ParsingType.DOUBLE, s(5))
          val frame = parseRegion(ParsingType.STRING, s(7))

          val otherValues = Array.fill[GValue](otherGtfSize)(GNull())
          otherValues(0) = source
          otherValues(1) = feature
          otherValues(2) = score
          otherValues(3) = frame


          val values = s(8) split semiCommaDelimiter
          values.foreach { value =>
            val split = value.trim.split(spaceDelimiter, 2)
            if (split.length == 2) {
              val attName = split(0).toUpperCase
              if (otherGtf.contains(attName)) {
                val attVal = split(1).trim.replaceAll("""^"(.+?)\"$""", "$1")
                val (parseType, arrayPos) = otherGtf(attName)
                otherValues(arrayPos) = parseRegion(parseType, attVal)
              }
              else
                logger.warn("Skipped the attribute value, it is not defined in schema " + value)
            }
          }
          otherValues
        case _ =>
          //if other position is defined then convert every element into GValue with parseRegion, else return empty array
          otherPos.getOrElse(Array.empty).map { case (pos, parseType) => parseRegion(parseType, s(pos)) }
      }

      Some((GRecordKey(t._1,
        s(chrPos).trim,
        if (coordinateSystem == GMQLSchemaCoordinateSystem.OneBased) s(startPos).trim.toLong - 1 else s(startPos).trim.toLong,
        s(stopPos).trim.toLong,
        strandPos.map(s(_).trim) match {
          case Some("+") => '+'
          case Some("-") => '-'
          case _ => '*'
        }
      ), other))
    }
    catch {
      case e: Throwable =>

        if (!t._2.startsWith("#")) {

          // Launched exception
          var exceptionMessage = "The following region is not compliant with the provided schema: \n [" + t._2 + "]\n"

          val cols: Array[String] = t._2.split(delimiter, -1)
          val found_cols = cols.length
          val schema_cols = 4 + otherPos.getOrElse(Array()).length

          // Wrong number of columns
          if (found_cols != schema_cols) {
            exceptionMessage += "\n Expecting " + schema_cols + " columns, found " + found_cols + ". "
            if (found_cols <= 1) {
              exceptionMessage += "Check the spacing."
            }
          }

          // Casting exception
          if (e.isInstanceOf[IllegalArgumentException] || e.isInstanceOf[NumberFormatException]) {
            exceptionMessage += "\n Wrong type: " + e.getClass.getCanonicalName.replace("java.lang.", "") + " " + e.getMessage
          }

          throw ParsingException.create(exceptionMessage, e)

        } else {
          None
        }
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


object BroadProjParser extends BedParser("\t", 0, 1, 2, None, Some(Array((3, ParsingType.STRING)))) {

  schema = List(("name", ParsingType.STRING))
}

/**
  *
  * Parser for Chr, Start, Stop only (no Strand)
  *
  */
object BasicParser extends BedParser("\t", 0, 1, 2, None, None) {

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
    val XMLfile: InputStream =
      if (!fs.exists(new Path(dataset + (if (!dataset.endsWith("xml")) "/schema.xml" else ""))))
        fs.open(new Path(dataset + (if (!dataset.endsWith("xml")) "/test.schema" else "")))
      else
        fs.open(new Path(dataset + (if (!dataset.endsWith("xml")) "/schema.xml" else "")))
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
      case GMQLSchemaFormat.VCF => {
        parsingType = GMQLSchemaFormat.VCF

        if (coordinateSystem == GMQLSchemaCoordinateSystem.Default) coordinateSystem = GMQLSchemaCoordinateSystem.OneBased

        val valuesPositions = schema.zipWithIndex.flatMap { x =>
          val name = x._1._1
          if (checkCoordinatesName(name)) None
          else Some(x._2 + 2, x._1._2)
        }

        val valuesPositionsSchema = schema.flatMap { x =>
          val name = x._1
          if (checkCoordinatesName(name)) None
          else Some(x._1, x._2)
        }.toList

        val other: Array[(Int, ParsingType.Value)] = if (valuesPositions.length > 0)
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

        val valuesPositions: Array[(Int, ParsingType.Value)] = schema.flatMap { x =>
          val name = x._1.toUpperCase();
          if (name.equals("SEQNAME") || name.equals("SOURCE") || name.equals("FEATURE") || name.equals("FRAME") || name.equals("SCORE") || checkCoordinatesName(name)) None
          else Some(8, x._2)
        }

        val valuesPositionsSchema: Seq[(String, ParsingType.Value)] = schema.flatMap { x =>
          val name = x._1.toUpperCase();
          if (name.equals("SEQNAME") || name.equals("SOURCE") || name.equals("FEATURE") || name.equals("FRAME") || name.equals("SCORE") || checkCoordinatesName(name)) None
          else Some(x._1, x._2)
        }.toList


        val other: Array[(Int, ParsingType.Value)] = if (valuesPositions.length > 0)
          Array[(Int, ParsingType.Value)]((1, ParsingType.STRING), (2, ParsingType.STRING), (5, ParsingType.DOUBLE), (7, ParsingType.STRING)) ++ valuesPositions
        else
          Array((5, ParsingType.DOUBLE))

        chrPos = 0
        startPos = 3
        stopPos = 4
        strandPos = Some(6)
        otherPos = Some(other)

        this.schema = List(("source", ParsingType.STRING), ("feature", ParsingType.STRING), ("score", ParsingType.DOUBLE), ("frame", ParsingType.STRING)) ++ valuesPositionsSchema
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
          missing += 1;
          0
        }
        val startPosition = if (start.size > 0) start.head._2
        else {
          missing += 1;
          1
        }
        val stopPosition = if (stop.size > 0) stop.head._2
        else {
          missing += 1;
          2
        }
        val strPosition = if (strand.size > 0) Some(strand.head._2 + missing)
        else {
          logger.warn("Strand is not specified in the XML schema file, the default strand (which is * is selected.")
          None
        } //in this case strand considered not present

        val valuesPositions = schemaWithIndex.flatMap { x =>
          val name = x._1._1;
          if (checkCoordinatesName(name)) None
          else Some(x._2 + missing, x._1._2)
        }
        val valuesPositionsSchema = schemaWithIndex.flatMap { x =>
          val name = x._1._1;
          if (checkCoordinatesName(name)) None
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
  def checkCoordinatesName(fieldName: String): Boolean = {
    fieldName.toUpperCase().equals("CHROM") || fieldName.toUpperCase().equals("CHROMOSOME") ||
      fieldName.toUpperCase().equals("CHR") || fieldName.toUpperCase().equals("START") ||
      fieldName.toUpperCase().equals("STOP") || fieldName.toUpperCase().equals("LEFT") ||
      fieldName.toUpperCase().equals("RIGHT") || fieldName.toUpperCase().equals("END") ||
      fieldName.toUpperCase().equals("STRAND") || fieldName.toUpperCase().equals("STR")
  }
}


object BedScoreParser extends BedParser("\t", 0, 1, 2, None, Some(Array((3, ParsingType.DOUBLE)))) {
  schema = List(("score", ParsingType.DOUBLE))
}

object TestParser {
  def main(args: Array[String]): Unit = {
    //    other = {Tuple2[19]@19273}
    //    0 = {Tuple2@22029} "(1,STRING)"
    //    1 = {Tuple2@22030} "(2,STRING)"
    //    2 = {Tuple2@22031} "(5,DOUBLE)"
    //    3 = {Tuple2@22032} "(7,STRING)"
    //    4 = {Tuple2@22033} "(8,STRING)"
    //    5 = {Tuple2@22034} "(8,STRING)"
    //    6 = {Tuple2@22035} "(8,STRING)"
    //    7 = {Tuple2@22036} "(8,STRING)"
    //    8 = {Tuple2@22037} "(8,STRING)"
    //    9 = {Tuple2@22038} "(8,STRING)"
    //    10 = {Tuple2@22039} "(8,STRING)"
    //    11 = {Tuple2@22040} "(8,STRING)"
    //    12 = {Tuple2@22041} "(8,STRING)"
    //    13 = {Tuple2@22042} "(8,DOUBLE)"
    //    14 = {Tuple2@22043} "(8,STRING)"
    //    15 = {Tuple2@22044} "(8,DOUBLE)"
    //    16 = {Tuple2@22045} "(8,STRING)"
    //    17 = {Tuple2@22046} "(8,STRING)"
    //    18 = {Tuple2@22047} "(8,STRING)"
    val tuple = (6253431748398267672L, "chr1\tHAVANA\texon\t11869\t12227\t.\t+\t.\tgene_id \"ENSG00000223972.5\"; transcript_id \"ENST00000456328.2\"; gene_type \"transcribed_unprocessed_pseudogene\"; gene_status \"KNOWN\"; gene_name \"DDX11L1\"; transcript_type \"processed_transcript\"; transcript_status \"KNOWN\"; transcript_name \"DDX11L1-002\"; exon_number 1; exon_id \"ENSE00002234944.1\"; level 2; tag \"basic\"; transcript_support_level \"1\"; havana_gene \"OTTHUMG00000000961.2\"; havana_transcript \"OTTHUMT00000362751.1\";")
    //    val other: Array[(Int, ParsingType.Value)] = Array(())
    //    new BedParser("\t",0,3,4,Some(6),)
  }
}

