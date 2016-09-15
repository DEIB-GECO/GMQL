package it.polimi.genomics.core.exception

/**
 * Created by michelebertoni on 29/04/15.
 */
class SelectFormatException(msg:String) extends RuntimeException(msg)

object SelectFormatException{
  def create(msg: String) : SelectFormatException = new SelectFormatException(msg)
  def create(msg: String, cause: Throwable) = new SelectFormatException(msg).initCause(cause)
}
