package it.polimi.genomics.manager;

/**
 * Created by Abdulrahman Kaitoua on 11/09/15.
 * Email: abdulrahman.kaitoua@polimi.it
 *
 */
/**
  * GMQL Job status enum.
  *
  */
object Status extends Enumeration {
  type Status = Value
  val COMPILING: Status.Value = Value("COMPILING")
  val COMPILE_FAILED: Status.Value = Value("COMPILE_FAILED")
  val COMPILE_SUCCESS: Status.Value= Value("COMPILE_SUCCESS")
  val PENDING: Status.Value= Value("PENDING")
  val RUNNING : Status.Value= Value("RUNNING")
  val EXEC_SUCCESS: Status.Value= Value("EXEC_SUCCESS")
  val DS_CREATION_RUNNING: Status.Value= Value("DS_CREATION_RUNNING")
  val DS_CREATION_FAILED: Status.Value = Value("DS_CREATION_FAILED")
  val DS_CREATION_SUCCESS : Status.Value= Value("DS_CREATION_SUCCESS")
  val EXEC_FAILED: Status.Value= Value("EXEC_FAILED")
  val EXEC_STOPPED: Status.Value= Value("EXEC_STOPPED")
  val SUCCESS : Status.Value= Value("SUCCESS")
}
