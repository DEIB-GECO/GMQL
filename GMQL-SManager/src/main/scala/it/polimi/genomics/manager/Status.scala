package it.polimi.genomics.manager;

/**
 * Created by Abdulrahman Kaitoua on 11/09/15.
 * Email: abdulrahman.kaitoua@polimi.it
 *
 */

object Status extends Enumeration {
  type Status = Value
  val COMPILING = Value("COMPILING")
  val COMPILE_FAILED = Value("COMPILE_FAILED")
  val COMPILE_SUCCESS= Value("COMPILE_SUCCESS")
  val PENDING= Value("PENDING")
  val RUNNING = Value("RUNNING")
  val EXEC_SUCCESS= Value("EXEC_SUCCESS")
  val DS_CREATION_RUNNING= Value("DS_CREATION_RUNNING")
  val DS_CREATION_FAILED = Value("DS_CREATION_FAILED")
  val DS_CREATION_SUCCESS = Value("DS_CREATION_SUCCESS")
  val EXEC_FAILED= Value("EXEC_FAILED")
  val SUCCESS = Value("SUCCESS")
}
