package it.polimi.genomics.scidb.test

import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.scidb.repository.{GmqlSciImporter, GmqlSciRepositoryManager}

/**
  * Created by Cattani Simone on 07/04/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object RepositoryTest
{
  def main(args: Array[String]): Unit =
  {
    val repository = new GmqlSciRepositoryManager
    //println(repository.fetch("BERT_JOIN_EXP"))

    println(repository.fetch("HG_NARROWPEAKS_10"));
  }
}
