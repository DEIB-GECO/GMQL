package it.polimi.genomics.GMQLServer

import it.polimi.genomics.core.DataStructures.Builtin.MetaAggregateFactory
import it.polimi.genomics.core.DataStructures.MetaAggregate.{MetaAggregateFunction}

object DefaultMetaAggregateFactory extends  MetaAggregateFactory{

  override def get(name: String, input : String, output: String): MetaAggregateFunction = {

    new MetaAggregateFunction {

      //fun should take the list of values of attribute inputAttributeNames[0]
      override val fun: (Array[Traversable[String]]) => String = name.toLowerCase match {
        case "sum" => get_sum()
        //case "avg" =>

      }

      //notice that regardless the fact it's a list, inputAttributeNames will always have exactly one element
      override val inputAttributeNames: List[String] = List(input)

      override val newAttributeName: String = output
    }


    //this should be the sum aggregate function,
    // not-tested!!!
    def get_sum() = {
      (x:Array[Traversable[String]]) =>

        //Only consider the elem at 0: Array will only have one element.
        x(0).toList.map(_.toDouble).sum.toString
    }


  }

}
