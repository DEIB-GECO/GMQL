package it.polimi.genomics.repository.GMQLExceptions

/**
  * Created by abdulrahman on 18/01/2017.
  */
case class GMQLNotValidDatasetNameException(message:String) extends RuntimeException(message)
{
  def this() = this("The data set name is not valid")
}

