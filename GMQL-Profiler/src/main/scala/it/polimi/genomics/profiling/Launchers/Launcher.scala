package it.polimi.genomics.profiling.Launchers

import it.polimi.genomics.profiling.Status


/**
  * Created by andreagulino on 06/04/17.
  */
trait Launcher {

  def profile(datasetPath: String): Status.Value;

}
