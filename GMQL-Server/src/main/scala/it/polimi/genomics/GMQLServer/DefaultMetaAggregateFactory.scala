package it.polimi.genomics.GMQLServer

import it.polimi.genomics.core.DataStructures.Builtin.MetaAggregateFactory
import it.polimi.genomics.core.DataStructures.MetaAggregate.{MetaAggregateFunction}

object DefaultMetaAggregateFactory extends  MetaAggregateFactory{

  val castExc = "GMQL Casting Exception – Could not parse"

/*
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
*/

  def get(name : String, input_name : String, output_name : Option[String]) = {
    name.toUpperCase() match {
      case "COUNT" => getCount(input_name,output_name)
      case "SUM" => getSum(input_name,output_name)
      case "MIN" => getMin(input_name,output_name)
      case "MAX" => getMax(input_name,output_name)
      case "AVG" => getAvg(input_name,output_name)
      case "BAG" => getBAG(input_name,output_name)
      case "BAGD" => getBAGD(input_name,output_name)
      case "STD" => getSTD(input_name,output_name)
      case "MEDIAN" => getMEDIAN(input_name,output_name)
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

  private def getSTD(input_name:String,new_name:Option[String]) = new MetaAggregateFunction {

    override val newAttributeName = if(new_name.isDefined) new_name.get else "STD"
    override val inputName: String = input_name
    override val fun: (Array[Traversable[String]]) => String = {
      (line) =>{
        val doubleVals = line.head.flatMap((value) => {
          val v1 = castDoubleOrString(value);
          if (v1.isInstanceOf[Double]) Some(v1.asInstanceOf[Double]) else None
        }).toArray
        if (!doubleVals.isEmpty) {
          val std = stdev(doubleVals)
          std.toString
        }
        else castExc
      }
    }
  }

  def avg(data: Array[Double]): Double = {
    if (data.length < 1)
      return Double.NaN
    data.sum / data.length
  }

  def stdev(data: Array[Double]): Double = {
    if (data.length < 2)
      return Double.NaN
    // average
    val mean: Double = avg(data)

    val sum = data.foldLeft(0.0)((sum, tail) => {
      val dif = tail - mean
      sum + dif * dif
    })

    Math.sqrt(sum / (data.length - 1))
  }

  private def getMEDIAN(input_name:String,new_name:Option[String]) = new MetaAggregateFunction {
    override val newAttributeName: String = if(new_name.isDefined) new_name.get else "MEDIAN"
    override val inputName: String = input_name
    override val fun: (Array[Traversable[String]]) => String = {
      (line) => {
        val values: List[Double] = line.head.flatMap{(value) =>
          val v1=castDoubleOrString(value);
          if (v1.isInstanceOf[Double]) Some(v1.asInstanceOf[Double]) else None
        }.toList.sorted
        if (!values.isEmpty) {
          if (values.length % 2 == 0) {
            val right = values.length / 2
            val left = (values.length / 2) - 1
            val res = (values(left) + values(left)) / 2
            res.toString
          }
          else {
            val res = values(values.length / 2)
            res.toString
          }
        }
        else castExc
      }
    }
  }

  private def getSum(input_name:String,new_name:Option[String]) = new MetaAggregateFunction {
    override val newAttributeName = if(new_name.isDefined) new_name.get else "SUM"
    override val inputName: String = input_name
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

  private def getCount(input_name:String,new_name:Option[String]) = new MetaAggregateFunction {

    override val newAttributeName = if(new_name.isDefined) new_name.get else "COUNT"
    override val inputName: String = input_name
    override val fun: (Array[Traversable[String]]) => String = {
      (line) =>{line.head.size.toString}
    }
  }

  private def getMin(input_name:String, new_name:Option[String]) = new MetaAggregateFunction {
    override val newAttributeName = if(new_name.isDefined) new_name.get else "MIN"
    override val inputName: String = input_name
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

  private def getMax(input_name:String, new_name:Option[String]) = new MetaAggregateFunction {
    override val newAttributeName = if(new_name.isDefined) new_name.get else "MAX"
    override val inputName: String = input_name
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

  private def getAvg(input_name:String, new_name:Option[String]) = new MetaAggregateFunction {

    override val newAttributeName = if(new_name.isDefined) new_name.get else "AVG"
    override val inputName: String = input_name
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

  private def getBAG(input_name:String, new_name:Option[String]) = new MetaAggregateFunction {

    override val newAttributeName = if(new_name.isDefined) new_name.get else "Bag"
    override val inputName: String = input_name
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

  private def getBAGD(input_name:String, new_name:Option[String]) = new MetaAggregateFunction {

    override val newAttributeName = if(new_name.isDefined) new_name.get else "BagD"
    override val inputName: String = input_name
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
