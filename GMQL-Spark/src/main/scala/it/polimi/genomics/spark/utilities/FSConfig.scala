package it.polimi.genomics.spark.utilities

import org.apache.hadoop.conf.Configuration

/**
  * Created by andreagulino on 08/11/17.
  */
object FSConfig {

  var conf : Configuration = null

  def getConf(): Configuration = {
    if( conf == null ) {
      conf = new Configuration()
    }
    conf
  }

  def setConf(c: Configuration): Unit = {
    conf = c
  }

}
