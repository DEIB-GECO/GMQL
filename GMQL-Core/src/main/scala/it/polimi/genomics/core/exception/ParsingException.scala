package it.polimi.genomics.core.exception

/**
 * Created by michelebertoni on 29/04/15.
 */

class ParsingException(msg:String) extends RuntimeException(msg)

object ParsingException{
  def create(msg: String) : ParsingException = new ParsingException(msg)
  def create(msg: String, cause: Throwable) = new ParsingException(msg).initCause(cause)
}
