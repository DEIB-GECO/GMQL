package it.polimi.genomics.scidb.exception


/**
  * Generic GMQL exception on SciDB implementation
  * @param message exception message
  * @param cause possible external cause
  */
abstract class GmqlSciException(message:String, cause:Throwable = null)
  extends Exception(message,cause)


// ------------------------------------------------------------
// ------------------------------------------------------------


/**
  * Indicates a general internal error
  * @param message exception message
  * @param cause possible external cause
  */
class InternalErrorGmqlSciException(message:String, cause:Throwable = null)
  extends GmqlSciException(message,cause)


/**
  * Indicates a general internal error
  * @param message exception message
  * @param cause possible external cause
  */
class IOErrorGmqlSciException(message:String, cause:Throwable = null)
  extends InternalErrorGmqlSciException(message,cause)


/**
  * Indicates that the required intermediate data is missing
  * @param message exception message
  * @param cause possible external cause
  */
class MissingDataGmqlSciException(message:String, cause:Throwable = null)
  extends GmqlSciException(message,cause)


/**
  * Indicates that the required intermediate data is missing
  * @param message exception message
  * @param cause possible external cause
  */
class UnsupportedOperationGmqlSciException(message:String, cause:Throwable = null)
  extends GmqlSciException(message,cause)