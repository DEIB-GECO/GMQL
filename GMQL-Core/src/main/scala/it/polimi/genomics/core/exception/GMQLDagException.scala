package it.polimi.genomics.core.exception

/**
  * Created by canakoglu on /11/717.
  */

class GMQLDagException(message:String) extends RuntimeException(message) {
  def this() = this("The DAG is not valid")
}
