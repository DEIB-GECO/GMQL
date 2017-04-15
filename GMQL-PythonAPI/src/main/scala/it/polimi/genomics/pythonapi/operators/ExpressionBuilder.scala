package it.polimi.genomics.pythonapi.operators
import it.polimi.genomics.core.DataStructures._
/**
  * Created by Luca Nanni on 11/04/17.
  * Email: luca.nanni@mail.polimi.it
  */

/**
  * This class enables the creation of complex region and meta conditions
  * based on the desires of the user
  * */
class ExpressionBuilder {

  def createMetaPredicate() = {
    // TEST
    MetadataCondition.Predicate("cell", MetadataCondition.META_OP.EQ, "K562")
  }

}
