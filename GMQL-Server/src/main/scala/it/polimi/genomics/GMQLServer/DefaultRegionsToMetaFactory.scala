package it.polimi.genomics.GMQLServer

import it.polimi.genomics.core.DataStructures.Builtin.ExtendFunctionFactory
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToMeta
import it.polimi.genomics.core._

/**
 * Created by pietro on 28/09/15.
 */
object DefaultRegionsToMetaFactory extends ExtendFunctionFactory {

  def get(name : String, output_name : Option[String]) = {
    name match {
      case "COUNT" => getCount(0,output_name)
      case _ => {
        var msg = "No nullary extend function with the given name (" + name + ") found."
        try {
          get(name, -255, output_name)
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

  def get(name : String, position : Int, output_name : Option[String]) = {
    name.toUpperCase() match {
      case "SUM" => getSum(position,output_name)
      case "MIN" => getMin(position,output_name)
      case "MAX" => getMax(position,output_name)
      case "AVG" => getAvg(position,output_name)
      case "BAG" => getBAG(position,output_name)
      case "STD" => getSTD(position,output_name)
      case "MEDIAN" => getMEDIAN(position,output_name)
      case "Q2" => getMEDIAN(position,output_name)
      case "Q1" => getQ1(position,output_name)
      case "Q3" => getQ3(position,output_name)
      case _ => throw new Exception("No extend function with the given name (" + name + ") found.")
    }
  }


  private def getSTD(position:Int,new_name:Option[String]) = new RegionsToMeta {

    override val newAttributeName = if(new_name.isDefined) new_name.get else "STD"
    override val inputIndex: Int = position
    override val associative : Boolean = false
    override val fun: (List[GValue]) => GValue = {
      (line) =>{
        //sqrt(avg(x^2) - avg(x)^2)
       /* val doubleVals = line.map((gvalue) =>gvalue.asInstanceOf[GDouble].v).toArray*/
       val doubleVals = line.flatMap((gvalue) => {
         if (gvalue.isInstanceOf[GDouble]) Some(gvalue.asInstanceOf[GDouble].v) else None
       }).toArray
        if (!doubleVals.isEmpty) {
          //
          //        val ss = line.map((gvalue) => {val s = gvalue.asInstanceOf[GDouble].v;(math.pow(s,2),s)})
          //        val dd = ss.reduce((x,y)=>(x._1 + y._1, x._2 + y._1));
          //        GDouble(math.sqrt((dd._1/line.length) -math.pow((dd._2/line.length),2)))
          GDouble(stdev(doubleVals))
        }
        else GNull()
      }
    }
    override val funOut: (GValue,Int) => GValue = {(v1,v2)=>v1}
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

  private def getMEDIAN(position:Int,new_name:Option[String]) = new RegionsToMeta {

    override val newAttributeName = if(new_name.isDefined) new_name.get else "MEDIAN"
    override val inputIndex: Int = position
    override val associative : Boolean = false
    override val fun: (List[GValue]) => GValue = {
      (line) => {
        /*val values: List[Double] = line.map{ gvalue => val s = gvalue.asInstanceOf[GDouble].v;s}.sorted*/
        val values: List[Double] = line.flatMap((gvalue) => {
          if (gvalue.isInstanceOf[GDouble]) Some(gvalue.asInstanceOf[GDouble].v) else None
        }).sorted
        if (!values.isEmpty) {
          if (/*line*/values.length % 2 == 0) {
            val right = /*line*/values.length / 2
            val left = (/*line*/values.length / 2) - 1
            GDouble((values(left) + values(left)) / 2)
          }
          else
            GDouble(values(/*line*/values.length / 2))
        }
        else GNull()
      }
    }
    override val funOut: (GValue,Int) => GValue = {(v1,v2)=>v1}
  }

  private def getQ3(position:Int,new_name:Option[String]) = new RegionsToMeta {

    override val newAttributeName = if(new_name.isDefined) new_name.get else "Q3"
    override val inputIndex: Int = position
    override val associative : Boolean = false
    override val fun: (List[GValue]) => GValue = {
      (line) => {
        /*val values: List[Double] = line.map{ gvalue => val s = gvalue.asInstanceOf[GDouble].v;s}.sorted*/
        val values: List[Double] = line.flatMap((gvalue) => {
          if (gvalue.isInstanceOf[GDouble]) Some(gvalue.asInstanceOf[GDouble].v) else None
        }).sorted
        if (!values.isEmpty) {
          val right = /*line*/ values.length / 2
          val left = (/*line*/ values.length / 2) - 1
          val up = right + right / 2
          if ( /*line*/ values.length % 2 == 0)
            GDouble((values(up) + values(up - 1)) / 2)
          else {
            GDouble(values(up))
          }
        }
        else GNull()
      }
    }
    override val funOut: (GValue,Int) => GValue = {(v1,v2)=>v1}
  }

  private def getQ1(position:Int,new_name:Option[String]) = new RegionsToMeta {

    override val newAttributeName = if(new_name.isDefined) new_name.get else "Q1"
    override val inputIndex: Int = position
    override val associative : Boolean = false
    override val fun: (List[GValue]) => GValue = {
      (line) => {
        /*val values: List[Double] = line.map{ gvalue => val s = gvalue.asInstanceOf[GDouble].v;s}.sorted*/
        val values: List[Double] = line.flatMap((gvalue) => {
          if (gvalue.isInstanceOf[GDouble]) Some(gvalue.asInstanceOf[GDouble].v) else None
        }).sorted
        if (!values.isEmpty) {
          val right = /*line*/values.length / 2
          val left = (/*line*/values.length / 2) - 1
          val down = right / 2
          if (/*line*/values.length % 2 == 0)
            GDouble((values(down) + values(down - 1)) / 2)
          else {
            GDouble(values(down))
          }
        }
        else GNull()
      }
    }
    override val funOut: (GValue,Int) => GValue = {(v1,v2)=>v1}
  }

  private def getSum(position:Int,new_name:Option[String]) = new RegionsToMeta {

    override val newAttributeName = if(new_name.isDefined) new_name.get else "SUM"
    override val inputIndex: Int = position
    override val associative : Boolean = true
    override val fun: (List[GValue]) => GValue = {
      (line) =>{/*val ss = line.map((gvalue) => {val s = gvalue.asInstanceOf[GDouble].v;s})*/
      val ss = line.flatMap((gvalue) => {if (gvalue.isInstanceOf[GDouble]) Some(gvalue.asInstanceOf[GDouble].v) else None})
        if (!ss.isEmpty) {
          val dd = ss.reduce(_ + _);
          GDouble(dd)
        }
        else GNull()
      }
    }
    override val funOut: (GValue,Int) => GValue = {(v1,v2)=>v1}
  }

  private def getCount(position:Int,new_name:Option[String]) = new RegionsToMeta {

    override val newAttributeName = if(new_name.isDefined) new_name.get else "COUNT"
    override val inputIndex: Int = position
    override val associative : Boolean = true
    override val fun: (List[GValue]) => GValue = {
      (line) =>{GDouble(line.length)
      }
    }
    override val funOut: (GValue,Int) => GValue = {(v1,v2)=>GDouble(v2)}
  }

  private def getMin(position:Int, new_name:Option[String]) = new RegionsToMeta {
    override val newAttributeName = if(new_name.isDefined) new_name.get else "MIN"
    override val inputIndex: Int = position
    override val associative : Boolean = true
    override val fun: (List[GValue]) => GValue = {
      (line) => /*GDouble(line.map((gvalue) => gvalue.asInstanceOf[GDouble].v).reduce( (x,y) =>Math.min(x,y)))*/
        val lines = line.flatMap((gvalue) => if (gvalue.isInstanceOf[GDouble]) Some(gvalue.asInstanceOf[GDouble].v) else None)
        if(!lines.isEmpty)
          GDouble(lines.reduce( (x,y) =>Math.min(x,y)))
        else GNull()
    }
    override val funOut: (GValue,Int) => GValue = {(v1,v2)=>v1}
  }

  private def getMax(position:Int, new_name:Option[String]) = new RegionsToMeta {
    override val newAttributeName = if(new_name.isDefined) new_name.get else "MAX"
    override val inputIndex: Int = position
    override val associative : Boolean = true
    override val fun: (List[GValue]) => GValue = {
      (line) => /*GDouble(line.map((gvalue) => gvalue.asInstanceOf[GDouble].v).reduce( (x,y) =>Math.max(x,y)))*/
        val lines = line.flatMap((gvalue) => if (gvalue.isInstanceOf[GDouble]) Some(gvalue.asInstanceOf[GDouble].v) else None)
        if(!lines.isEmpty)
          GDouble(lines.reduce( (x,y) =>Math.max(x,y)))
        else GNull()
    }
    override val funOut: (GValue,Int) => GValue = {(v1,v2)=>v1}
  }

  private def getAvg(position:Int, new_name:Option[String]) = new RegionsToMeta {

    override val newAttributeName = if(new_name.isDefined) new_name.get else "AVG"
    override val inputIndex: Int = position
    override val associative : Boolean = true
    override val fun: (List[GValue]) => GValue = {
      (line) =>
//        val len = line.size.toDouble
//        if(len != 0)
          /*GDouble((line.map((gvalue) => gvalue.asInstanceOf[GDouble].v).reduce(_ + _))/*/len*/)*/
//        else
//          GDouble(0)
        val lines = line.flatMap((gvalue) => if (gvalue.isInstanceOf[GDouble]) Some(gvalue.asInstanceOf[GDouble].v) else None)
        if(!lines.isEmpty)
          GDouble((lines.reduce(_ + _)))
        else
          GNull()
    }
    override val funOut: (GValue,Int) => GValue = {(v1,v2)=>/*if(v1.asInstanceOf[GDouble].v>0) GDouble(v1.asInstanceOf[GDouble].v/v2)else GDouble(0)*/
      if (v2 > 0 && !v1.isInstanceOf[GNull]) GDouble(v1.asInstanceOf[GDouble].v / v2) else GNull()}
  }

  private def getBAG(position:Int, new_name:Option[String]) = new RegionsToMeta {

    override val newAttributeName = if(new_name.isDefined) new_name.get else "Bag"
    override val inputIndex: Int = position
    override val associative : Boolean = true
    override val fun: (List[GValue]) => GValue = {
      (line) =>

        if(line.nonEmpty)
          GString((line.map((gvalue) => {
            gvalue match{
              case GString(v) => List(v)
              case GDouble(v) => List(v.toString)
              case GInt(v) => List(v.toString)
              case GNull() => List("_")
            }
          }).distinct.reduce((a, b) => a ++ b)).sorted.mkString(" ")) // TODO sorted is added only for comparation reason, we can get rid of it

        //if(line.size>0)
        //  GString((line.map((gvalue) => gvalue.asInstanceOf[GString].v).reduce(_ + _)).sorted) // TODO sorted is added only for comparation reason, we can get rid of it
        else
          GString(" ")

    }
    override val funOut: (GValue,Int) => GValue = {(v1,v2)=>v1}
  }

}
