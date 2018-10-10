package it.polimi.genomics.core

/**
  * Created by abdulrahman on 23/01/2017.
  */
object ImplementationPlatform extends Enumeration{
  type platform = Value
  val FLINK, SPARK, SCIDB,FEDERATED = Value
}

