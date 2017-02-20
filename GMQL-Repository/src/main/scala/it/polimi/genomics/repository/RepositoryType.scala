package it.polimi.genomics.repository

/**
  * Created by abdulrahman on 15/02/2017.
  */
object RepositoryType extends Enumeration{
  type REPOSITORY_TYPE = Value;
  val LOCAL, HDFS, REMOTE = Value
}

object DatasetOrigin extends Enumeration{
  type Origin = Value;
  val GENERATED, IMPORTED = Value
}
