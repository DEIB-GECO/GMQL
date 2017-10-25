package it.polimi.genomics.GMQLServer

import it.polimi.genomics.core.DataStructures.Builtin.MetaAggregateFactory
import it.polimi.genomics.core.DataStructures.MetaAggregate.{MetaAggregateFunction}

object DefaultMetaAggregateFactory extends  MetaAggregateFactory{

  val castExc = "GMQL Casting Exception – Could not parse"

  def get(name : String, output : Option[String]) = {
    name match {
      case "COUNT" => getCount("",output)
      case _ => {
        var msg = "No nullary aggregate function with the given name (" + name + ") found."
        try {
          get(name, "", output)
          msg = msg.concat(
            List(" Hint: an unary function named", name,
              "exists; try to use", name, "( <field-identifier> )").mkString(" "))
        } catch {
          case e:Exception =>
        }
        throw new Exception(msg)
      }
    }
  }

  def get(name : String, position : String, output_name : Option[String]) = {
    name.toUpperCase() match {
      case "SUM" => getSum(position,output_name)
      case "MIN" => getMin(position,output_name)
      case "MAX" => getMax(position,output_name)
      case "AVG" => getAvg(position,output_name)
      case "BAG" => getBAG(position,output_name)
      case "BAGD" => getBAGD(position,output_name)
      case _ => throw new Exception("No aggregate function with the given name (" + name + ") found.")
    }
  }
/*
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
  }*/

  private def getSum(position:String,new_name:Option[String]) = new MetaAggregateFunction {
    override val newAttributeName = if(new_name.isDefined) new_name.get else "SUM"
    override val inputName: String = position
    override val fun: (Array[Traversable[String]]) => String = {
      (line) =>{
        val ss = line.head.flatMap{(value) =>
          val v1=castDoubleOrString(value);
          if (v1.isInstanceOf[Double]) Some(v1.asInstanceOf[Double]) else None}
        if (!ss.isEmpty) {
          val dd = ss.reduce(_ + _);
          dd.toString
        }
        else castExc
      }
    }
  }

  private def getCount(position:String,new_name:Option[String]) = new MetaAggregateFunction {

    override val newAttributeName = if(new_name.isDefined) new_name.get else "COUNT"
    override val inputName: String = position
    override val fun: (Array[Traversable[String]]) => String = {
      (line) =>{line.head.size.toString}
    }
  }

  private def getMin(position:String, new_name:Option[String]) = new MetaAggregateFunction {
    override val newAttributeName = if(new_name.isDefined) new_name.get else "MIN"
    override val inputName: String = position
    override val fun: (Array[Traversable[String]]) => String = {
      (line) =>
        val lines = line.head.flatMap{(value) =>
          val v1=castDoubleOrString(value);
          if (v1.isInstanceOf[Double]) Some(v1.asInstanceOf[Double]) else None}
        if(!lines.isEmpty)
          lines.reduce( (x,y) =>Math.min(x,y)).toString
        else castExc
    }
  }

  private def getMax(position:String, new_name:Option[String]) = new MetaAggregateFunction {
    override val newAttributeName = if(new_name.isDefined) new_name.get else "MAX"
    override val inputName: String = position
    override val fun: (Array[Traversable[String]]) => String = {
      (line) =>
        val lines = line.head.flatMap{(value) =>
          val v1=castDoubleOrString(value);
          if (v1.isInstanceOf[Double]) Some(v1.asInstanceOf[Double]) else None}
        if(!lines.isEmpty)
          lines.reduce( (x,y) =>Math.max(x,y)).toString
        else castExc
    }
  }

  private def getAvg(position:String, new_name:Option[String]) = new MetaAggregateFunction {

    override val newAttributeName = if(new_name.isDefined) new_name.get else "AVG"
    override val inputName: String = position
    override val fun: (Array[Traversable[String]]) => String = {
      (line) =>
        val lines = line.head.flatMap{(value) =>
          val v1=castDoubleOrString(value);
          if (v1.isInstanceOf[Double]) Some(v1.asInstanceOf[Double]) else None}
        if(!lines.isEmpty)
          (lines.reduce(_ + _) / lines.size).toString
        else
          castExc
    }
  }

  private def getBAG(position:String, new_name:Option[String]) = new MetaAggregateFunction {

    override val newAttributeName = if(new_name.isDefined) new_name.get else "Bag"
    override val inputName: String = position
    override val fun: (Array[Traversable[String]]) => String = {
      (list) => {
        if (list.nonEmpty) {
          list.head.toArray.sorted.map((value) => {
            if (!value.isEmpty) value else "."
          }).mkString(",")
        }
        else "."
      }
    }
  }

  private def getBAGD(position:String, new_name:Option[String]) = new MetaAggregateFunction {

    override val newAttributeName = if(new_name.isDefined) new_name.get else "BagD"
    override val inputName: String = position
    override val fun: (Array[Traversable[String]]) => String = {
      (list) =>{
        if (list.nonEmpty)
          list.head.toArray.distinct.sorted.map((value)=> {
            if (!value.isEmpty) value else "."
          }).mkString(",")
        else "."
      }
    }
  }

  def castDoubleOrString(value : Any) : Any = {
    try{
      value.toString.toDouble
    } catch {
      case e : Throwable => value.toString
    }
  }

}
