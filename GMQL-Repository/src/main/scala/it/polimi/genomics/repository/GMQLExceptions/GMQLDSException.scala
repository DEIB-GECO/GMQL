package it.polimi.genomics.repository.GMQLExceptions

/**
  * Created by Abdulrahman Kaitoua on 11/04/16.
  */

/**
  * General exception
  * @param message
  */
class GMQLDSException (message:String) extends RuntimeException{
  def this() = this("DataSet Error")
}

/**
  * GMQL data set is not found is exception that is thrown when the dataset is not found in the dataset manager.
  *
  * @param message
  */
class GMQLDSNotFound(message:String) extends GMQLDSException(message)
{
  def this() = this("DataSet is not found")
}

/**
  * User name is not found in this repository should rise this exception
  *
  * @param message
  */
class GMQLUserNotFound(message:String) extends GMQLDSException(message)
{
  def this() = this("User Name is not found")
}

/**
  * Sample is not found in the repository (or in the URL if it is URL)
  * @param message
  */
class GMQLSampleNotFound(message:String) extends GMQLDSException(message)
{
  def this() = this("Sample is not found")
}