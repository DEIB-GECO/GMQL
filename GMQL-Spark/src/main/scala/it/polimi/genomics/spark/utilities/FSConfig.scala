package it.polimi.genomics.spark.utilities

import org.apache.hadoop.conf.Configuration

/**
  * Created by andreagulino on 08/11/17.
  */
object FSConfig {

  var conf : Configuration = null

  def getConf(): Configuration = {

    val projId      = "gmql-188714"
    val credentials = "/usr/src/myapp/credentials.json"

    if( conf == null ) {
      conf = new Configuration()
      conf.set("hadoop.google.cloud.auth.service.account.enable","true")
      conf.set("hadoop.google.cloud.auth.service.account.json.keyfile", credentials)
      conf.set("hadoop.fs.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      conf.set("hadoop.fs.AbstractFileSystem.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      conf.set("hadoop.fs.gs.project.id",projId)
    }
    conf
  }

  def setConf(c: Configuration): Unit = {
    conf = c
  }

}
