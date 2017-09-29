package it.polimi.genomics.profiling

/**
  * Created by andreagulino on 06/04/17.
  */

object Status extends Enumeration {
  type Status = Value
  val PENDING: Status.Value  = Value("PENDING")
  val RUNNING : Status.Value = Value("RUNNING")
  val SUCCESS : Status.Value = Value("SUCCESS")
  val FAILED : Status.Value  = Value("FAILED" )
}
