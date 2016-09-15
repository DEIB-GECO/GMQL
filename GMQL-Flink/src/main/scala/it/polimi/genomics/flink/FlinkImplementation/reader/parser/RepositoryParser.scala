package it.polimi.genomics.flink.FlinkImplementation.reader.parser

import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.repository.util.Utilities
import org.slf4j.LoggerFactory

import scala.xml.XML

/**
 * Created by michelebertoni on 16/09/15.
 */
object RepositoryParser {
  private val logger = LoggerFactory.getLogger(this.getClass);
  final val GTF = "gtf"
  final val VCF = "vcf"

  //val schema = List(("name", ParsingType.STRING), ("score", ParsingType.DOUBLE))
  def apply(dataset: String) : DelimiterSeparatedValuesParser = {


    val XMLfile = if(!Utilities.getInstance().checkDSNameinPublic(dataset))
      Utilities.getInstance().RepoDir+Utilities.USERNAME+"/schema/"+dataset+".schema"
    else
      Utilities.getInstance().RepoDir+"public"+"/schema/"+dataset+".schema"

    println(XMLfile)
    var schematype = "tab"
    var schema: Array[(String, ParsingType.Value)] = null

        try {
    val schemaXML = XML.loadFile(XMLfile);
    val cc = (schemaXML \\ "field")
    schematype = (schemaXML \\ "gmqlSchema").head.attribute("type").get.head.text
    schema = cc.map(x => (x.text.toUpperCase.trim, attType(x.attribute("type").get.head.text))).toArray
        } catch {
          case x: Throwable => logger.error(x.getMessage); throw new RuntimeException(x.getMessage)
        }

    schematype.trim.toLowerCase() match {
      case VCF =>{


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

        val parser = new DelimiterSeparatedValuesParser('\t', 0, 1, 1, None, Some(other))

        parser.schema = valuesPositionsSchema
        parser.parsingType = VCF
        parser
      }
      case GTF => {


        val valuesPositions = schema.flatMap { x => val name = x._1;
          if (name.equals("SOURCE") ||name.equals("FEATURE") ||name.equals("FRAME") ||name.equals("SCORE") || checkCoordinatesName(name)) None
          else Some(8, x._2)
        }

        val valuesPositionsSchema = schema.flatMap { x => val name = x._1;
          if (name.equals("SOURCE") ||name.equals("FEATURE") ||name.equals("FRAME") ||name.equals("SCORE") || checkCoordinatesName(name)) None
          else Some(x._1, x._2)
        }.toList

        val other = if (valuesPositions.length > 0)
          (5, ParsingType.DOUBLE) +: valuesPositions
        else
          Array((5, ParsingType.DOUBLE))

        val parser = new DelimiterSeparatedValuesParser('\t', 0, 3, 4, Some(6), Some(other))



        parser.schema = List(("score", ParsingType.DOUBLE)) ++ valuesPositionsSchema
        parser.parsingType = GTF
        parser
      }

      case _ => {

        val schemaWithIndex = schema.zipWithIndex
        val chrom = schemaWithIndex.filter(x => (x._1._1.equals("CHROM") || x._1._1.equals("CHROMOSOME") || x._1._1.equals("CHR")))
        val start = schemaWithIndex.filter(x => (x._1._1.equals("START") || x._1._1.equals("LEFT")))
        val stop = schemaWithIndex.filter(x => (x._1._1.equals("STOP") || x._1._1.equals("RIGHT") || x._1._1.equals("END")))
        val strand = schemaWithIndex.filter(x => (x._1._1.equals("STR") || x._1._1.equals("STRAND")))

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
        val strPosition = if (strand.size > 0) Some(strand.head._2)
        else {
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
        val parser = new DelimiterSeparatedValuesParser('\t', chromPosition, startPosition, stopPosition, strPosition, Some(valuesPositions))



        parser.schema = valuesPositionsSchema.toList
        parser
      }
    }
  }

  def attType(x: String) = x.toUpperCase match {
    case "STRING" => ParsingType.STRING
    case "CHAR" => ParsingType.STRING
    case "CHARACTAR" => ParsingType.STRING
    case _ => ParsingType.DOUBLE
  }

    def checkCoordinatesName(fieldName:String):Boolean={
      (fieldName.equals("CHROM") || fieldName.equals("CHROMOSOME") ||
        fieldName.equals("CHR") || fieldName.equals("START") ||
        fieldName.equals("STOP") || fieldName.equals("LEFT") ||
        fieldName.equals("RIGHT") || fieldName.equals("END") ||
        fieldName.equals("STRAND") || fieldName.equals("STR"))
    }
}
