package it.polimi.genomics.scidbapi.exception

/**
  * Generic SciDB exception
  * @param message exception message
  * @param cause possible external cause
  */
abstract class SciException(message:String, cause:Throwable = null)
  extends Exception(message,cause)


// ------------------------------------------------------------
// ------------------------------------------------------------


/**
  * Indicates that was be required an operation using wrong
  * configuration or types
  * @param message exception message
  * @param cause possible external cause
  */
class IllegalOperationSciException(message:String, cause:Throwable = null)
  extends SciException(message,cause)

/**
  * Indicates that was be required an operation that is not
  * already supported by the api
  * @param message exception message
  * @param cause possible external cause
  */
class UnsupportedOperationSciException(message:String, cause:Throwable = null)
  extends SciException(message,cause)


// ------------------------------------------------------------


/**
  * Indicates that was be required a resource that doesn't exist
  * @param message exception message
  * @param cause possible external cause
  */
class ResourceNotFoundSciException(message:String, cause:Throwable = null)
  extends SciException(message,cause)


// ------------------------------------------------------------


/**
  * Indicates that a confict on some resource is occured
  * @param message exception message
  * @param cause possible external cause
  */
class ResourceConflictSciException(message:String, cause:Throwable = null)
  extends SciException(message,cause)

/**
  * Indicates that a property (like an attribute or a dimension)
  * is ambiguos, or already exists
  * @param message exception message
  * @param cause possible external cause
  */
class PropertyAmbiguitySciException(message:String, cause:Throwable = null)
  extends ResourceConflictSciException(message,cause)


// ------------------------------------------------------------


/**
  * Indicates a syntax error in the code
  * @param message exception message
  * @param cause possible external cause
  */
class SyntaxErrorSciException(message:String, cause:Throwable = null)
  extends SciException(message,cause)