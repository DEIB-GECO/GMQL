package it.polimi.genomics.profiling.Launchers

/**
  * Created by andreagulino on 06/04/17.
  */
class RemoteLauncher extends Launcher {
  override def profile(datasetPath: String): _root_.it.polimi.genomics.profiling.Status.Value = {
    println("Profiling on RemoteLauncher")
    _root_.it.polimi.genomics.profiling.Status.FAILED
  }
}
