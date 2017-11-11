package it.polimi.genomics.core

import it.polimi.genomics.core.DataStructures.IRVariable

/**
  * Created by Luca Nanni on 04/11/17.
  * Email: luca.nanni@mail.polimi.it
  */
case class DAGWrapper(dag: List[IRVariable]) extends Serializable
