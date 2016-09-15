package it.polimi.genomics.scidb.test.test

import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.scidb.repository.GmqlSciRepositoryManager

/**
  * Created by Cattani Simone on 06/04/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object TestImporter
{
  def main(args: Array[String]): Unit =
  {
    WONG_DATA
  }

  def WONG_DATA =
  {
    println("\n-----------------------------------------------")
    println("-- WONG_DATA ----------------------------------")
    println("-----------------------------------------------")

    val repository = new GmqlSciRepositoryManager

    repository.importation(
      "ANN_ORG",
      "/Users/cattanisimone/Desktop/output/gmql_testing/annotations",
      List(
        ("chr", ParsingType.STRING, 1),
        ("left", ParsingType.INTEGER, 2),
        ("right", ParsingType.INTEGER, 3),
        ("strand", ParsingType.STRING, 4),
        ("gene", ParsingType.STRING, 5),
        ("score", ParsingType.DOUBLE, 6)
      ), "tsv", None
    )

    repository.importation(
      "BED_ORG",
      "/Users/cattanisimone/Desktop/output/gmql_testing/beds",
      List(
        ("chr", ParsingType.STRING, 1),
        ("left", ParsingType.INTEGER, 2),
        ("right", ParsingType.INTEGER, 3),
        ("score", ParsingType.DOUBLE, 4)
      ), "tsv", Some("bed")
    )
  }
}
