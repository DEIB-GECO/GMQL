package it.polimi.genomics.core.exception

/**
  * Created by andreagulino on 03/11/17.
  */
class UserExceedsQuota(message:String) extends RuntimeException(message) {
  def this() = this("The user disk quota is exceeded")
}
