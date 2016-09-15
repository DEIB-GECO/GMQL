package it.polimi.genomics.scidb.test.experiment

import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.scidb.repository.GmqlSciImporter

/**
  * Created by Cattani Simone on 02/06/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object ImporterExperiment
{
  def main(args: Array[String]): Unit =
  {
    importExp_01
    //importTmp
  }

  def importTmp = {
    GmqlSciImporter.importDS(
      "EXP_01_EXP",
      "/home/scidb/dataset/experiment/EXP_01_REF3K",
      List(
        ("chr", ParsingType.STRING, 1),
        ("left", ParsingType.INTEGER, 2),
        ("right", ParsingType.INTEGER, 3),
        ("strand", ParsingType.STRING, 4),
        ("score", ParsingType.DOUBLE, 5)
      ), "tsv"
    )
  }


  def importExp_01 =
  {
    val names = List(
      "EXP_01_REF3K", "EXP_01_REF3KM","EXP_01_REF70K",
      "EXP_01_EXP1H_1H","EXP_01_EXP1H_2H","EXP_01_EXP1H_5H",
      "EXP_01_EXP1H_1K","EXP_01_EXP1H_2K","EXP_01_EXP1H_5K",
      "EXP_01_EXP1H_10K","EXP_01_EXP1H_20K","EXP_01_EXP1H_50K"/*,
      "EXP_01_EXP70K_1H","EXP_01_EXP70K_2H","EXP_01_EXP70K_5H",
      "EXP_01_EXP70K_1K","EXP_01_EXP70K_2K","EXP_01_EXP70K_5K",
      "EXP_01_EXP70K_10K","EXP_01_EXP70K_20K","EXP_01_EXP70K_50K"*/
    )

    for(name <- names){
      GmqlSciImporter.importDS(
        name,
        "/home/scidb/dataset/experiment/"+name,
        List(
          ("chr", ParsingType.STRING, 1),
          ("left", ParsingType.INTEGER, 2),
          ("right", ParsingType.INTEGER, 3),
          ("strand", ParsingType.STRING, 4),
          ("score", ParsingType.DOUBLE, 5)
        ), "tsv"
      )
    }
  }
}
