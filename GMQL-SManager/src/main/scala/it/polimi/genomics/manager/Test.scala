package it.polimi.genomics.manager

import it.polimi.genomics.core.GDMSUserClass.GDMSUserClass
import it.polimi.genomics.core.{GDMSUserClass, GMQLSchemaFormat, GMQLScript, ImplementationPlatform}
import it.polimi.genomics.repository.{Utilities => RepoUtilities}


object Test extends App {


  val username = "canakoglu"
  val confFolder = "/Users/canakoglu/GMQL-sources/GMQL-WEB/conf/gmql_conf"

  // Create repository
  RepoUtilities.confFolder = confFolder
  val repository = RepoUtilities().getRepository()

  //  // List Datasets
  //  val datasets: util.List[IRDataSet] = repository.listAllDSs("andreagulino")
  //
  //  for (dataset: IRDataSet <- repository.listAllDSs(username)) {
  //    println(dataset.position)
  //  }


  val query = "DATA_SET_VAR = SELECT(at:it.polimi.Location0) it.polimi.Example_Dataset_0;\nMATERIALIZE DATA_SET_VAR INTO RESULT_DS;"
  val queryName = "queryName"
  val outputFormat = GMQLSchemaFormat.TAB
  val userClass = GDMSUserClass.ADMIN


//  compile

  execute


  def compile = {
    val gmqlScript = GMQLScript(query, queryName)
    val gmqlContext = GMQLContext(ImplementationPlatform.FEDERATED, repository, outputFormat, username = username, checkQuota = false)
    val job: GMQLJob = new GMQLJob(gmqlContext, gmqlScript, gmqlContext.username)
    job.compile()
    job
  }


  def execute = {
    val server = GMQLExecute()
    val job = registerJob(username, userClass, query, queryName, outputFormat)
    server.execute(job)
  }

  private def registerJob(username: String, userClass: GDMSUserClass, query: String, queryName: String, outputFormat: GMQLSchemaFormat.Value) = {
    val server = GMQLExecute()
    val gmqlScript = GMQLScript(query, queryName)
    val gmqlContext = GMQLContext(ImplementationPlatform.FEDERATED, repository, outputFormat, username = username, userClass = userClass, checkQuota = true)
    server.registerJob(gmqlScript, gmqlContext, "")
  }
}
