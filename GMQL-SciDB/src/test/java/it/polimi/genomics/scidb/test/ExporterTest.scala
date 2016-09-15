package it.polimi.genomics.scidb.test

import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.scidb.repository.GmqlSciExporter

/**
  * Created by Cattani Simone on 02/03/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object ExporterTest
{
  def main(args: Array[String]): Unit =
  {
    println("\n-----------------------------------------------")
    println("-- EXPORT TEST --------------------------------")
    println("-----------------------------------------------")

    /*GmqlSciExporter.exportDS(
      "TEST_NARROW_PEAK",
      "/home/scidb/test/real/np_exp",
      List(
        ("name", ParsingType.STRING),
        ("score", ParsingType.STRING),
        ("signal", ParsingType.DOUBLE),
        ("pValue", ParsingType.DOUBLE),
        ("qValue", ParsingType.DOUBLE),
        ("peak", ParsingType.INTEGER)
      ), "tsv"
    )*/

    exportBertJoin1

  }

  def exportBertJoin1 =
  { import scala.collection.JavaConverters._
    GmqlSciExporter.exportDS(
      "BERT_JOIN_REF",
      "/home/scidb/test/real/bertEXP/join/ref",
      List(
        ("val1", ParsingType.INTEGER),
        ("val2", ParsingType.STRING)
      ).asJava, "tsv"
    )

    GmqlSciExporter.exportDS(
      "BERT_JOIN_EXP",
      "/home/scidb/test/real/bertEXP/join/exp",
      List(
        ("val1", ParsingType.INTEGER),
        ("val2", ParsingType.STRING)
      ).asJava, "tsv"
    )
  }
}
