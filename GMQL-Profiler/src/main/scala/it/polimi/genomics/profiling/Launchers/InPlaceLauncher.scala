package it.polimi.genomics.profiling.Launchers

import it.polimi.genomics.profiling.Profilers.Profiler
import it.polimi.genomics.profiling.Status

/**
  * Created by andreagulino on 06/04/17.
  */
class InPlaceLauncher extends Launcher {
  override def profile(datasetPath: String): Status.Value = {

//    println("Profiling on InPlaceLauncher")
//
//    val profiler = new Profiler()
//    profiler.profile(datasetPath)
    Status.SUCCESS


  }
}
