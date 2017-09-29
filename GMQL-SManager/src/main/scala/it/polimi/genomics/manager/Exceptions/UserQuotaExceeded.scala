package it.polimi.genomics.manager.Exceptions

/**
  * Created by andreagulino on 28/09/17.
  */
class UserQuotaExceeded (message: String) extends RuntimeException{
  def this() = this("The user disk quota is exceeded")
}