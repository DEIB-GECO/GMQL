package it.polimi.genomics.core.Debug

import it.polimi.genomics.core.DataStructures.GMQLOperator

import java.util.UUID.randomUUID

case class OperatorDescr(name: GMQLOperator.Value, params: Option[Map[String, String]] = None) {

  var id = randomUUID()

}