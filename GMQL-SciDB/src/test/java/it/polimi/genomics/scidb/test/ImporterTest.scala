package it.polimi.genomics.scidb.test

import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.scidb.repository.GmqlSciImporter

/**
  * Created by Cattani Simone on 29/02/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object ImporterTest
{
  def main(args: Array[String]): Unit =
  {
    println("\n-----------------------------------------------")
    println("-- IMPORT TEST --------------------------------")
    println("-----------------------------------------------")

    /*GmqlSciImporter.importDS(
      "TEST_NARROW_PEAK",
      "/home/scidb/test/real/np",
      List(
        ("chr", ParsingType.STRING, 1),
        ("start", ParsingType.STRING, 2),
        ("stop", ParsingType.INTEGER, 3),
        ("strand", ParsingType.STRING, 4),
        ("name", ParsingType.STRING, 5),
        ("score", ParsingType.STRING, 6),
        ("signal", ParsingType.DOUBLE, 7),
        ("pValue", ParsingType.DOUBLE, 8),
        ("qValue", ParsingType.DOUBLE, 9),
        ("peak", ParsingType.INTEGER, 10)
      ), "tsv"
    )*/

    //importBertJoin1
    //importBertOrder
    importBertMap
    //importBertCover

    //importBertGrouprd

  }


  def importBertGrouprd =
  {
    GmqlSciImporter.importDS(
      "DEV_GROUPRD_REF",
      "/home/scidb/dataset/dev/bert/grouprd",
      List(
        ("chr", ParsingType.STRING, 1),
        ("left", ParsingType.STRING, 2),
        ("right", ParsingType.INTEGER, 3),
        ("strand", ParsingType.STRING, 6),
        ("val1", ParsingType.INTEGER, 5),
        ("val2", ParsingType.STRING, 9)
      ), "tsv"
    )
  }

  def importBertCover =
  {
    GmqlSciImporter.importDS(
      "DEV_COVER_REF",
      "/home/scidb/dataset/dev/bert/cover",
      List(
        ("chr", ParsingType.STRING, 1),
        ("left", ParsingType.STRING, 2),
        ("right", ParsingType.INTEGER, 3),
        ("strand", ParsingType.STRING, 6),
        ("val1", ParsingType.INTEGER, 5),
        ("val2", ParsingType.STRING, 9)
      ), "tsv"
    )
  }


  def importBertMap =
  {
    GmqlSciImporter.importDS(
      "DEV_MAP_REF",
      "/home/scidb/dataset/dev/bert/map/ref",
      List(
        ("chr", ParsingType.STRING, 1),
        ("left", ParsingType.STRING, 2),
        ("right", ParsingType.INTEGER, 3),
        ("strand", ParsingType.STRING, 6),
        ("val1", ParsingType.INTEGER, 5),
        ("val2", ParsingType.STRING, 9)
      ), "tsv"
    )

    GmqlSciImporter.importDS(
      "DEV_MAP_EXP",
      "/home/scidb/dataset/dev/bert/map/exp",
      List(
        ("chr", ParsingType.STRING, 1),
        ("left", ParsingType.STRING, 2),
        ("right", ParsingType.INTEGER, 3),
        ("strand", ParsingType.STRING, 6),
        ("val1", ParsingType.INTEGER, 5),
        ("val2", ParsingType.STRING, 9)
      ), "tsv"
    )
  }

  def importBertJoin1 =
  {
    GmqlSciImporter.importDS(
      "DEV_JOIN_REF",
      "/home/scidb/dataset/dev/bert/join/ref",
      List(
        ("chr", ParsingType.STRING, 1),
        ("left", ParsingType.STRING, 2),
        ("right", ParsingType.INTEGER, 3),
        ("strand", ParsingType.STRING, 6),
        ("val1", ParsingType.INTEGER, 5),
        ("val2", ParsingType.STRING, 9)
      ), "tsv"
    )

    GmqlSciImporter.importDS(
      "DEV_JOIN_EXP",
      "/home/scidb/dataset/dev/bert/join/exp",
      List(
        ("chr", ParsingType.STRING, 1),
        ("left", ParsingType.STRING, 2),
        ("right", ParsingType.INTEGER, 3),
        ("strand", ParsingType.STRING, 6),
        ("val1", ParsingType.INTEGER, 5),
        ("val2", ParsingType.STRING, 9)
      ), "tsv"
    )
  }

  def importBertOrder =
  {
    GmqlSciImporter.importDS(
      "DEV_ORDER_REF",
      "/home/scidb/dataset/dev/bert/order",
      List(
        ("chr", ParsingType.STRING, 1),
        ("left", ParsingType.STRING, 2),
        ("right", ParsingType.INTEGER, 3),
        ("strand", ParsingType.STRING, 6),
        ("val1", ParsingType.INTEGER, 5),
        ("val2", ParsingType.STRING, 9)
      ), "tsv"
    )
  }


}
