package it.polimi.genomics.manager

import java.util

import it.polimi.genomics.core.DataStructures.IRDataSet
import it.polimi.genomics.repository.{GMQLRepository, Utilities => RepoUtilities}
import collection.JavaConversions._


object Test {

  def main(args: Array[String]): Unit = {

    val username   = "federated"
    val confFolder = "/Users/andreagulino/Projects/GMQL-WEB/conf/gmql_conf/"

    // Create repository
    RepoUtilities.confFolder = confFolder
    val repo = RepoUtilities().getRepository()

    // List Datasets
    val datasets: util.List[IRDataSet] = repo.listAllDSs("andreagulino")

    for (dataset: IRDataSet <- repo.listAllDSs(username)) {
      println( dataset.position )
    }



  }
}
