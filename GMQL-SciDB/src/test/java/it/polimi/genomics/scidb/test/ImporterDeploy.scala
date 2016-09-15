package it.polimi.genomics.scidb.test

import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.scidb.repository.{GmqlSciRepositoryManager, GmqlSciImporter}

/**
  * Created by Cattani Simone on 06/04/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object ImporterDeploy
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
      "DS_LIGHT",
      "/Users/cattanisimone/Desktop/output/bedsm",
      List(
        ("chr", ParsingType.STRING, 1),
        ("left", ParsingType.INTEGER, 2),
        ("right", ParsingType.INTEGER, 3),
        ("score", ParsingType.DOUBLE, 4)
      ), "tsv", Some("bed")
    )
  }
}
