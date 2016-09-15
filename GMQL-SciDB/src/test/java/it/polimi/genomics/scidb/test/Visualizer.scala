package it.polimi.genomics.scidb.test

import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.scidb.operators.GDS
import it.polimi.genomics.scidb.repository.{GmqlSciExporterGTF, GmqlSciRepositoryManager}
import it.polimi.genomics.scidb.utility.SchemaUtils
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.{SciStoredArray, SciArray}

/**
  * Created by Cattani Simone on 23/05/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object Visualizer
{
  def main(args: Array[String]): Unit =
  {
    val repository = new GmqlSciRepositoryManager
    repository.exportation(
      repository.fetch("TEST_QUERY_3").get,
      "/Users/cattanisimone/Desktop/exported/"
    )
  }
}
