package it.polimi.genomics.scidb.utility

import it.polimi.genomics.scidb.GmqlSciConfig
import it.polimi.genomics.scidbapi.SciAbstractArray
import it.polimi.genomics.scidbapi.script.SciScript

/**
  * This library provides some tools to simplify the
  * developing of the module
  */
object DebugUtils
{

  /**
    * Execute the array into scidb to test it
    *
    * @param array array to be tested
    * @param script context script
    */
  def exec(array : SciAbstractArray, context : SciScript) : Unit =
  {
    val script = new SciScript
    //script.statements = context.statements
    //script.queues = context.queues

    script.addStatement(array)

    script.flushQueue()

    script.export("../output/","utils.afl")
    script.run(
      GmqlSciConfig.scidb_server_ip,
      GmqlSciConfig.scidb_server_username,
      GmqlSciConfig.scidb_server_password,
      GmqlSciConfig.scidb_server_runtime_dir
    )
  }

}
