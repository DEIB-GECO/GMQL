import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.schema.DataType._
import it.polimi.genomics.scidbapi.schema.{Attribute, Dimension}
import it.polimi.genomics.scidbapi.script._

/**
  * Created by Cattani Simone on 21/02/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object ApiScriptTest
{

  def main(args: Array[String]): Unit =
  {
    println("\n-----------------------------------------------")
    println("-- API TEST -----------------------------------")
    println("-----------------------------------------------")

    test1

  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def test1 =
  {
    println("\n-- Test 1 -------------------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("x",0,Some(3),100,0), Dimension("y",0,Some(4),200,0) ),
      List[Attribute]( Attribute("attr",INT64) ),
      "TEST")

    var script1 = new SciScript

    val A2 = A1.apply((A("attr_neg"), UOP("-", A("attr")))).store("TEST_2")

    script1.addStatement(SciCreate(true, "TEST", A2.getDimensions(), A2.getAttributes()))
    script1.addStatement(A2)

    //script1.run("192.168.211.144","scidb","scidb","/home/scidb/runtime")

    //println(script1.query())

    script1.export("../output/","afl_export_03.afl")

  }

}
