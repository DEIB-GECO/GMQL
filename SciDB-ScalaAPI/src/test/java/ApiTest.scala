import it.polimi.genomics.scidbapi.SciArray
import it.polimi.genomics.scidbapi.SortingKeyword._
import it.polimi.genomics.scidbapi.aggregate.{AGGR, AGGR_COUNT, AGGR_MAX}
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.schema.DataType._
import it.polimi.genomics.scidbapi.schema.{Attribute, Dimension}

/**
  * Created by cattanisimone on 16/02/16.
  */
object ApiTest
{

  def main(args: Array[String]): Unit =
  {
    println("\n-----------------------------------------------")
    println("-- API TEST -----------------------------------")
    println("-----------------------------------------------")

    /*
    exceptionTest

    attributeTest
    dimensionTest

    expressionTest
    aggregateTest

    arrayModelTest
    storedArrayTest

    arrayAggregateTest
    arrayApplyTest
    arrayFilterTest
    arrayProjectTest
    arrayCastTest
    arrayRedimensionTest*/
    //arrayCrossjoinTest
    /*arrayMergeTest
    arrayRankTest
    arraySortTest */
    //arrayUniqTest

    //arrayIndexlookupTest
    //arrayInputTest
    //arraySaveTest

    //arrayUnpackTest
    libraryTest

  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def libraryTest =
  {
    println("\n-- Library ------------------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("sid",0,None,1000,0) ),
      List[Attribute]( Attribute("chr_ref",STRING) ),
      "A1")

    val fun = SciFunction("dim_hash", List(STRING, INT64), INT64)

    val A2 = A1.apply((A("test"), LIB(fun, A("chr_ref"), D("sid"))))

    println(A2.description())
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def arrayUnpackTest =
  {
    println("\n-- Array Unpack -------------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("sid",0,None,1000,0) ),
      List[Attribute]( Attribute("chr_ref",STRING) ),
      "A1")

    val A2 = A1.unpack(D("x"))

    println(A2.description())
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def arraySaveTest =
  {
    println("\n-- Array Save ---------------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("sid",0,None,1000,0) ),
      List[Attribute]( Attribute("chr_ref",STRING) ),
      "A1")

    val A2 = A1.save("/home/scidb/repo/annotations/file1.bed.meta", -2, "tsv")

    println(A2.description())
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def arrayInputTest =
  {
    println("\n-- Array Input --------------------------------")

    val A1 = SciArray.input(
      "/home/scidb/repo/annotations/file1.bed.meta",
      List(Dimension("x", 0, None, 100, 0)),
      List(Attribute("name",STRING), Attribute("value",STRING)),
      -2, "tsv"
    )

    println(A1.description())
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def arrayIndexlookupTest =
  {
    println("\n-- Array Index lookup -------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("sid",0,None,1000,0) ),
      List[Attribute]( Attribute("chr_ref",STRING) ),
      "A1")

    val Aindex = new SciArray(
      List[Dimension]( Dimension("chr",0,None,1000,0) ),
      List[Attribute]( Attribute("chr_ref",STRING,false) ),
      "Aindex")

    val A3 = A1.index_lookup(Aindex, A("chr_ref"))

    println(A3)
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def arrayUniqTest =
  {
    println("\n-- Array Uniq ---------------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("chr",0,None,1000,0) ),
      List[Attribute]( Attribute("pValue",STRING) ),
      "A1")

    val A3 = A1.uniq()

    println(A3)
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def arraySortTest =
  {
    println("\n-- Array Sort ---------------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("chr",0,None,1000,0), Dimension("start",0,Some(100),1000,0), Dimension("stop",0,None,1000,0) ),
      List[Attribute]( Attribute("pValue",STRING), Attribute("qValue",DOUBLE) ),
      "A1")

    val A3 = A1.sort((A("qValue"),asc),(A("pValue"),desc))(100)

    println(A3)
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def arrayRankTest =
  {
    println("\n-- Array Rank ---------------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("chr",0,None,1000,0), Dimension("start",0,Some(100),1000,0), Dimension("stop",0,None,1000,0) ),
      List[Attribute]( Attribute("pValue",STRING), Attribute("qValue",DOUBLE) ),
      "A1")

    val A3 = A1.rank(A("pValue"))(D("chr"),D("start"))

    println(A3)
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def arrayMergeTest =
  {
    println("\n-- Array Merge --------------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("chr",0,None,1000,0), Dimension("start",0,Some(100),1000,0), Dimension("stop",0,None,1000,0) ),
      List[Attribute]( Attribute("pValue",DOUBLE), Attribute("qValue",DOUBLE) ),
      "A1")

    val A2 = new SciArray(
      List[Dimension]( Dimension("chr",0,None,1000,0), Dimension("sstart",0,Some(1000000),1000,0), Dimension("stop",0,None,1000,0) ),
      List[Attribute]( Attribute("piValue",DOUBLE), Attribute("quValue",DOUBLE) ),
      "A2")

    val A3 = A1.merge(A2)

    println(A3)
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def arrayCrossjoinTest =
  {
    println("\n-- Array Cross Join ---------------------------")

    val sid_ANCHOR_D      = Dimension("sid_ANCHOR",1,None,1000,0)
    val sid_EXPERIMENT_D  = Dimension("sid_EXPERIMENT",1,None,1000,0)
    val sid_RESULT_A      = Attribute("sid_RESULT", INT64)

    val metajoin_dimensions_D = List(sid_ANCHOR_D, sid_EXPERIMENT_D)
    val metajoin_attributes_A = List(sid_RESULT_A)

    val METAJOIN = new SciArray(metajoin_dimensions_D, metajoin_attributes_A, "METAJOIN")

    val ANCHOR_prepared = new SciArray(
      List[Dimension](
        Dimension("sid_ANC", 1, None, 1000, Dimension.DEFAULT_OVERLAP),
        Dimension("x_ANC", 0, None, 100, Dimension.DEFAULT_OVERLAP),
        Dimension("chr_ANC", 0, None, 100, Dimension.DEFAULT_OVERLAP)
      ),  List[Attribute](
        Attribute("start_ANC",INT64),
        Attribute("stop_ANC",INT64),
        Attribute("strand_ANC",INT64)
      ), "ANCHOR_prepared")

    val EXPERIMENT_prepared = new SciArray(
      List[Dimension](
        Dimension("sid_EXP", 1, None, 1000, Dimension.DEFAULT_OVERLAP),
        Dimension("x_EXP", 0, None, 100, Dimension.DEFAULT_OVERLAP),
        Dimension("chr_EXP", 0, None, 100, Dimension.DEFAULT_OVERLAP)
      ),  List[Attribute](
        Attribute("start_EXP",INT64),
        Attribute("stop_EXP",INT64),
        Attribute("strand_EXP",INT64)
      ), "EXPERIMENT_prepared")

    println(METAJOIN.getDimensions() + " - " + METAJOIN.getAttributes())
    println(ANCHOR_prepared.getDimensions() + " - " + ANCHOR_prepared.getAttributes())
    println(EXPERIMENT_prepared.getDimensions() + " - " + EXPERIMENT_prepared.getAttributes())

    val PUREJOIN = METAJOIN
      .cross_join(ANCHOR_prepared, "METAJOIN", "ANCHOR")(                                                               // join metajoin sid with the correct anchor
        (D("METAJOIN."+sid_ANCHOR_D.name), D("ANCHOR.sid_ANC")))
      .cross_join(EXPERIMENT_prepared, "ANCHOR", "EXPERIMENT")(                                                         // join metajoin sid with the correct experiment
        (D("ANCHOR."+sid_EXPERIMENT_D.name), D("EXPERIMENT.sid_EXP")),
        (D("ANCHOR.chr_ANC"), D("EXPERIMENT.chr_EXP")))

    println(PUREJOIN.getDimensions() + " - " + PUREJOIN.getAttributes())
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def arrayRedimensionTest =
  {
    println("\n-- Array Resimension ---------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("chr",0,None,1000,0), Dimension("start",0,None,1000,0), Dimension("stop",0,None,1000,0) ),
      List[Attribute]( Attribute("pValue",DOUBLE), Attribute("qValue",DOUBLE) ),
      "A1")

    val A2 = A1.redimension(
      List[Dimension]( Dimension("start",0,None,1000,0), Dimension("stop",0,None,1000,0) ),
      List[Attribute]( Attribute("chr",INT64), Attribute("counted",INT64) ),
      false,
      AGGR_COUNT(A("pValue"),A("counted"))
    )

    println(A2)
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def arrayCastTest =
  {
    println("\n-- Array Cast ----------------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("chr",0,None,1000,0), Dimension("start",0,None,1000,0), Dimension("stop",0,None,1000,0) ),
      List[Attribute]( Attribute("pValue",DOUBLE), Attribute("qValue",DOUBLE) ),
      "A1")

    val A2 = A1.cast(
      List[Dimension]( Dimension("A_chr",0,None,1000,0), Dimension("A_start",0,None,1000,0), Dimension("A_stop",0,None,1000,0) ),
      List[Attribute]( Attribute("A_pValue",DOUBLE), Attribute("A_qValue",DOUBLE) )
    )

    println(A2)
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def arrayApplyTest =
  {
    println("\n-- Array Apply ---------------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("chr",0,None,1000,0), Dimension("start",0,None,1000,0), Dimension("stop",0,None,1000,0) ),
      List[Attribute]( Attribute("pValue",DOUBLE), Attribute("qValue",DOUBLE) ),
      "A1")

    val A2 = A1.apply((A("yValue"), OP(A("pValue"), "+", A("qValue"))),
                      (A("wValue"), FUN("ceil", OP(A("pValue"), "-", A("qValue")))))

    println(A2)
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def arrayProjectTest =
  {
    println("\n-- Array Project ------------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("chr",0,None,1000,0), Dimension("start",0,None,1000,0), Dimension("stop",0,None,1000,0) ),
      List[Attribute]( Attribute("pValue",DOUBLE), Attribute("qValue",DOUBLE) ),
      "A1")

    val A2 = A1.project(A("pValue"),A("qValue"))

    println(A2)
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def arrayAggregateTest =
  {
    println("\n-- Array Aggregate ----------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("chr",0,None,1000,0), Dimension("start",0,None,1000,0), Dimension("stop",0,None,1000,0) ),
      List[Attribute]( Attribute("pValue",DOUBLE), Attribute("qValue",DOUBLE) ),
      "A1")

    val A2 = A1.aggregate(AGGR_COUNT(A("pValue"),A("counted")), AGGR_MAX(A("pValue")))()

    println(A2)
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def arrayFilterTest =
  {
    println("\n-- Array Filter -------------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("chr",0,None,1000,0), Dimension("start",0,None,1000,0), Dimension("stop",0,None,1000,0) ),
      List[Attribute]( Attribute("pValue",DOUBLE), Attribute("qValue",DOUBLE) ),
      "A1")

    val A2 = A1.filter( AND( OP(A("pValue"), "<", UOP("-",V(100))), OP(A("qValue"), ">", V(2.3))) )
    val A3 = A2.filter( OP(D("chr"),"=",V(2)) )

    println(A2)
    println(A3)
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def storedArrayTest =
  {
    println("\n-- Stored Array --------------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("chr",0,None,1000,0), Dimension("start",0,None,1000,0), Dimension("stop",0,None,1000,0) ),
      List[Attribute]( Attribute("pValue",DOUBLE), Attribute("qValue",DOUBLE) ),
      "A1")
      .filter( OP(D("chr"),"=",V(2)) )
      .apply((A("yValue"), OP(A("pValue"), "+", A("qValue"))),
             (A("wValue"), FUN("ceil", OP(A("pValue"), "-", A("qValue")))))

    val A2 = A1.store("A2")
    println(A2)

    val A3 = A2.reference().project(A("yValue"))
    println(A3)
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def arrayModelTest =
  {
    println("\n-- Array Model --------------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("chr",0,None,1000,0), Dimension("start",0,None,1000,0), Dimension("stop",0,None,1000,0) ),
      List[Attribute]( Attribute("pValue",DOUBLE), Attribute("qValue",DOUBLE) ),
      "A1")

    println(A1)
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def aggregateTest =
  {
    println("\n-- Aggregate ----------------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("chr",0,None,1000,0), Dimension("start",0,None,1000,0), Dimension("stop",0,None,1000,0) ),
      List[Attribute]( Attribute("pValue",DOUBLE), Attribute("qValue",DOUBLE) ),
      "A1")

    val aggr = AGGR_COUNT(A("pValue"),A("counted")).eval((A1.getDimensions(), A1.getAttributes()))
    println( aggr )

    val aggr2 = AGGR("count",A("pValue"),A("counted")).eval((A1.getDimensions(), A1.getAttributes()))
    println( aggr2 )
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def expressionTest =
  {
    println("\n-- Expression ---------------------------------")

    val A1 = new SciArray(
      List[Dimension]( Dimension("chr",0,None,1000,0), Dimension("start",0,None,1000,0), Dimension("stop",0,None,1000,0) ),
      List[Attribute]( Attribute("pValue",DOUBLE), Attribute("qValue",DOUBLE) ),
      "A1")

    val exp = OP(A("pValue"),"+",V(2)).eval((A1.getDimensions(), A1.getAttributes()))
    println( exp )

    val exp2 = FUN("abs",V(-34)).eval((A1.getDimensions(), A1.getAttributes()))
    println( exp2 )

    val exp3 = IF(OP(A("pValue"),">",V(0.3)), A("pValue"), V(0)).eval((A1.getDimensions(), A1.getAttributes()))
    println( exp3 )

    val exp4 = C_INT64(V("test")).eval((A1.getDimensions(), A1.getAttributes()))
    println( exp4 )
    val exp5 = CAST(V("test"),DOUBLE).eval((A1.getDimensions(), A1.getAttributes()))
    println( exp5 )
    val exp6 = CAST(V(23),DOUBLE).eval((A1.getDimensions(), A1.getAttributes()))
    println( exp6 )
    val exp7 = CAST(V(23),STRING).eval((A1.getDimensions(), A1.getAttributes()))
    println( exp7 )
    val exp8 = CAST(V(23.3),INT64).eval((A1.getDimensions(), A1.getAttributes()))
    println( exp8 )
    val exp9 = CAST(V(23.4),STRING).eval((A1.getDimensions(), A1.getAttributes()))
    println( exp9 )

  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def dimensionTest =
  {
    println("\n-- Dimension ----------------------------------")

    println( Dimension("chr",0,Some(1000),100,0) )

    println( Dimension("start",0,None,1000,0))
    println( Dimension("start",0,None,1000,0).description() )
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def attributeTest =
  {
    println("\n-- Attribute ----------------------------------")

    println( Attribute("pValue", DOUBLE) )
    println( Attribute("pValue", DOUBLE).description() )
  }


  // ------------------------------------------------------------
  // ------------------------------------------------------------


  def exceptionTest =
  {
    //throw new IllegalOperationSciException("testo")
  }

}
