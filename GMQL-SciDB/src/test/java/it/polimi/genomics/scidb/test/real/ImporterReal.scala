package it.polimi.genomics.scidb.test.real

import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.scidb.repository.{GmqlSciAcceleratedImporter, GmqlSciImporter}

/**
  * Created by Cattani Simone on 25/05/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object ImporterReal
{
  def main(args: Array[String]): Unit =
  {
    importNarrowTest(8)
    //importNarrow
  }


  def importPromoters =
  {
    GmqlSciImporter.importDS(
      "PROMOTERS",
      "/home/scidb/dataset/deploy/PROMOTERS",
      List(
        ("chr", ParsingType.STRING, 1),
        ("left", ParsingType.INTEGER, 2),
        ("right", ParsingType.INTEGER, 3),
        ("strand", ParsingType.STRING, 6),
        ("zero", ParsingType.INTEGER, 5)
      ), "tsv"
    )
  }

  def importGenes =
  {
    GmqlSciImporter.importDS(
      "GENES",
      "/home/scidb/dataset/deploy/GENES",
      List(
        ("chr", ParsingType.STRING, 1),
        ("left", ParsingType.INTEGER, 2),
        ("right", ParsingType.INTEGER, 3),
        ("strand", ParsingType.STRING, 6),
        ("zero", ParsingType.INTEGER, 5)
      ), "tsv"
    )
  }

  def importNarrowTest(size:Int) =
  {
    GmqlSciImporter.importDS(
      "HG_NARROWPEAKS_"+size,
      "/home/scidb/dataset/deploy/HG_NARROWPEAKS/test_"+size,
      List(
        ("chr", ParsingType.STRING, 1),
        ("left", ParsingType.INTEGER, 2),
        ("right", ParsingType.INTEGER, 3),
        ("score1", ParsingType.DOUBLE, 5),
        ("score2", ParsingType.DOUBLE, 7)
      ), "tsv", Some("bed")
    )
  }

  def importNarrow =
  {
    for(i <- 11 to 20)
    {
      GmqlSciAcceleratedImporter.importDS(
        (i*50),
        "HG_NARROWPEAKS_"+(i*50),
        "/home/scidb/dataset/deploy/HG_NARROWPEAKS/narrow",
        List(
          ("chr", ParsingType.STRING, 1),
          ("left", ParsingType.INTEGER, 2),
          ("right", ParsingType.INTEGER, 3),
          ("score1", ParsingType.DOUBLE, 5),
          ("score2", ParsingType.DOUBLE, 7)
        ), "tsv", Some("bed")
      )
    }
  }

}
