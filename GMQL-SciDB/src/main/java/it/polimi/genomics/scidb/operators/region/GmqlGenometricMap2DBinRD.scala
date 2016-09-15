package it.polimi.genomics.scidb.operators.region

import it.polimi.genomics.core.DataStructures.RegionAggregate.R2RAggregator
import it.polimi.genomics.scidb.GmqlSciConfig
import it.polimi.genomics.scidb.evaluators.AggregateEvaluator
import it.polimi.genomics.scidb.operators.{GDS, GmqlRegionOperator, GmqlMetaJoinOperator}
import it.polimi.genomics.scidb.utility.DebugUtils
import it.polimi.genomics.scidbapi.{SciStoredArray, SciArray}
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.schema.Attribute
import it.polimi.genomics.scidbapi.script.SciCommandType._
import it.polimi.genomics.scidbapi.script.{SciOperation, SciCreate, SciCommand, SciScript}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Created by Cattani Simone on 19/05/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
class GmqlGenometricMap2DBinRD(metajoin : GmqlMetaJoinOperator,
                               reference : GmqlRegionOperator,
                               experiment : GmqlRegionOperator,
                               aggregates : List[_<:R2RAggregator])
  extends GmqlRegionOperator
{

  val bin_size   = 14000000 //10000
  val bin_max    = 250000000 //100000

  // ------------------------------------------------------------
  // ------------------------------------------------------------

  /**
    * Apply the it.polimi.genomics.scidb.test.operator producing the result, it requires the
    * child nodes results if necessary
    *
    * @param script Context script
    */
  override def apply(script: SciScript): SciArray =
  {
    val REFERENCE = reference.compute(script)
    val EXPERIMENT = experiment.compute(script)

    val METAJOIN = metajoin.compute(script)

    println("2D BIN: "+bin_size)

    // ------------------------------------------------
    // ------------------------------------------------

    val intersectionDimensions = List(
      GDS.sid_ANCHOR_D,
      GDS.sid_EXPERIMENT_D,
      GDS.enumeration_D.label("ANCHOR"),
      GDS.chr_D.label("ANCHOR"),
      GDS.enumeration_D.label("EXPERIMENT")
    )

    val intersectionAttributes = List(
      GDS.sid_D.label("RESULT").toAttribute(),
      GDS.left_A.label("ANCHOR"),
      GDS.right_A.label("ANCHOR"),
      GDS.strand_A.label("ANCHOR")
    ) ::: REFERENCE.getAttributes().map(_.label("ANCHOR")) ::: List(
      GDS.left_A.label("EXPERIMENT"),
      GDS.right_A.label("EXPERIMENT"),
      GDS.strand_A.label("EXPERIMENT")
    ) ::: EXPERIMENT.getAttributes().map(_.label("EXPERIMENT"))

    val operations = aggregates.map(item =>
      AggregateEvaluator(
        item.function_identifier,
        EXPERIMENT.getAttributes()(item.input_index).label("EXPERIMENT").e,
        (if(item.output_name.isDefined) A(item.output_name.get)
        else A(item.function_identifier+"_"+EXPERIMENT.getAttributes()(item.input_index).label("EXPERIMENT").name))
      )
    )

    val aggregators = operations.map(_._1).reduce((l1,l2) => l1:::l2)
    val aggregatorsAtrributes = aggregators.map(item => {
      val (_, datatype, name) = item.eval((intersectionDimensions, intersectionAttributes))
      Attribute(name, datatype)
    })

    val expressions = operations.map(e => if(e._2.isDefined) List(e._2.get) else List()).reduce((l1,l2) => l1:::l2)
    val expressionsAttributes = expressions.map(item => {
      val (_, datatype) = item._2.eval((intersectionDimensions, intersectionAttributes:::aggregatorsAtrributes))
      Attribute(item._1.attribute, datatype)
    })

    val requiredAttributes = aggregates.map(item =>
      (if(item.output_name.isDefined) A(item.output_name.get)
      else A(item.function_identifier+"_"+EXPERIMENT.getAttributes()(item.input_index).label("EXPERIMENT").name))
    )
    val resultAttributes = (aggregatorsAtrributes ::: expressionsAttributes)
      .filter(attr => requiredAttributes.contains(attr.e))


    // ------------------------------------------------
    // ------------------------------------------------

    val prev = new SciScript
    prev.statements = script.statements
    script.freeStatements()
    script.addStatement(new SciCommand(set_timer))
    if(!GmqlSciConfig.scidb_server_verbouse)
      script.addStatement(new SciCommand(set_no_fetch))
    script.flushQueue()
    script.closeQueue()

    val RESULT = new SciStoredArray(
      GDS.regions_dimensions_D,
      REFERENCE.getAttributes() ::: resultAttributes,
      this._storing_name.getOrElse(prev.getTmpName +"_"+ this.getClass.getSimpleName +"_MapInternal"),
      this._storing_name.getOrElse(prev.getTmpName +"_"+ this.getClass.getSimpleName +"_MapInternal")
    )

    prev.addStatement(SciCreate(this._storing_temp,
      RESULT.getAnchor(),
      RESULT.getDimensions(),
      RESULT.getAttributes()))

    if(this._storing_temp == false){
      this._stored = false
    }

    if( GmqlSciConfig.scidb_server_debug )
      prev.export("../output/","map_prev.afl")

    if( GmqlSciConfig.scidb_server_on || true)
      prev.run(
        GmqlSciConfig.scidb_server_ip,
        GmqlSciConfig.scidb_server_username,
        GmqlSciConfig.scidb_server_password,
        GmqlSciConfig.scidb_server_runtime_dir
      )

    // ------------------------------------------------
    // ------------------------------------------------

    val left_REF    = GDS.left_D.label("ANCHOR").toAttribute().e
    val right_REF   = GDS.right_D.label("ANCHOR").toAttribute().e

    val left_EXP    = GDS.left_D.label("EXPERIMENT").toAttribute().e
    val right_EXP   = GDS.right_D.label("EXPERIMENT").toAttribute().e

    val strand_REF  = GDS.strand_D.label("ANCHOR").toAttribute().e
    val strand_EXP  = GDS.strand_D.label("EXPERIMENT").toAttribute().e

    // ------------------------------------------------
    // ------------------------------------------------

    val bins =
      //for(l <- 0 to (math.ceil(bin_max/bin_size).toInt - 1);                      // ALL
      //    r <- l to (math.ceil(bin_max/bin_size).toInt - 1))
      //for(l <- 0 to (math.ceil(bin_max/bin_size).toInt - 1);                      // PROMOTERS
      //    r <- l to Math.min(l+1, (math.ceil(bin_max/bin_size).toInt - 1)))
      for(l <- 0 to (math.ceil(bin_max/bin_size).toInt - 1);                      // GENES
          r <- l to Math.min(l+math.ceil(25000000/bin_size).toInt, (math.ceil(bin_max/bin_size).toInt - 1)))
      //for(l <- 9 to 9; r <- 9 to 9)
        yield (l,r)

    val scidb_start: Long = System.currentTimeMillis

    val all = -1
    def producer() = {
      val list = bins
        .map(bin => Future {

          val R = REFERENCE
            .between(
              (0,all),(0,all),   // (0,4),(0,24)
              (bin._1*bin_size, (bin._1+1)*bin_size),
              (bin._2*bin_size, (bin._2+1)*bin_size),
              (0,2),(0,all)
            )

          val E = EXPERIMENT
            .between(
              (0,all),(0,all),
              (0, (bin._2+1)*bin_size),
              (bin._1*bin_size, bin_max),
              (0,2),(0,all)
            )

          val JOINED =
            if (false)
              prepare(R, "ANCHOR")
                .cross_join(prepare(E, "EXPERIMENT"), "ANCHOR", "EXPERIMENT")(
                  (GDS.chr_D.label("ANCHOR").alias("ANCHOR").e, GDS.chr_D.label("EXPERIMENT").alias("EXPERIMENT").e))
                .apply((GDS.sid_RESULT_A.e, OP(OP(GDS.sid_ANCHOR_D.e, "*", V(10000)), "+", GDS.sid_EXPERIMENT_D.e)))
            else
              METAJOIN
                .cross_join(prepare(R, "ANCHOR"), "METAJOIN", "ANCHOR")(
                  (GDS.sid_ANCHOR_D.alias("METAJOIN").e, GDS.sid_ANCHOR_D.alias("ANCHOR").e))
                .cross_join(prepare(E, "EXPERIMENT"), "ANCHOR", "EXPERIMENT")(
                  (GDS.sid_EXPERIMENT_D.alias("ANCHOR").e, GDS.sid_EXPERIMENT_D.alias("EXPERIMENT").e),
                  (GDS.chr_D.label("ANCHOR").alias("ANCHOR").e, GDS.chr_D.label("EXPERIMENT").alias("EXPERIMENT").e))

          val INTERSECTED = JOINED
            .filter(
              AND(
                OR( OP(strand_REF, "=", strand_EXP),                                                                          // filter only the regions with a compatible strands
                  OR( OP(strand_REF, "=", V(1)), OP(strand_EXP, "=", V(1)))),
                OR(                                                                                                           // filter on intersection
                  AND( OP(left_EXP, ">=", left_REF), OP(left_EXP, "<=", right_REF) ),
                  AND( OP(left_EXP, "<=", left_REF), OP(left_REF, "<=", right_EXP) )
                )
              )
            )

          val AGGREGATED = INTERSECTED
            .redimension(
              List(
                GDS.sid_D.label("RESULT"),
                GDS.chr_D.label("ANCHOR"),
                GDS.left_D.label("ANCHOR"),
                GDS.right_D.label("ANCHOR"),
                GDS.strand_D.label("ANCHOR"),
                GDS.enumeration_D.label("ANCHOR")
              ),
              R.getAttributes().map(_.label("ANCHOR")) ::: aggregatorsAtrributes,
              false,
              aggregators:_*
            )

          /*val TEST = INTERSECTED
            .redimension(
              List(
                GDS.sid_D.label("RESULT"),
                GDS.chr_D.label("ANCHOR"),
                GDS.left_D.label("ANCHOR"),
                GDS.right_D.label("ANCHOR"),
                GDS.strand_D.label("ANCHOR"),
                GDS.enumeration_D.label("ANCHOR")
              ),
              R.getAttributes().map(_.label("ANCHOR")) ::: aggregatorsAtrributes,
              false,
              aggregators:_*
            )   */

          val APPLYED = if(expressions.isEmpty) AGGREGATED else AGGREGATED
            .apply(expressions:_*)

          val BIN_RESULT = APPLYED
            .cast(GDS.regions_dimensions_D, R.getAttributes() ::: aggregatorsAtrributes ::: expressionsAttributes)
            .project((R.getAttributes() ::: resultAttributes).map(_.e):_*)

          val BIN = BIN_RESULT
            .insert(RESULT)


          //DebugUtils.exec(INTERSECTED.store("BASE"), script);
          //DebugUtils.exec(TEST, script);

          val thread = new SciScript
          thread.addStatement(new SciCommand(set_timer))
          if(!GmqlSciConfig.scidb_server_verbouse)
            thread.addStatement(new SciCommand(set_no_fetch))
          thread.addStatement(BIN)

          if( GmqlSciConfig.scidb_server_debug )
            thread.export("../output/map_thread/","map_thread_"+bin._1+"x"+bin._2+".afl")

          if( true && GmqlSciConfig.scidb_server_on )
            thread.run(
              GmqlSciConfig.scidb_server_ip,
              GmqlSciConfig.scidb_server_username,
              GmqlSciConfig.scidb_server_password,
              GmqlSciConfig.scidb_server_runtime_dir,
              bin._1+"x"+bin._2
            )
        })
      Future.sequence(list)
    }

    Await.result(producer, Duration.Inf)

    val scidb_stop: Long = System.currentTimeMillis

    println("INTERNAL TIME: "+(scidb_stop - scidb_start))

    // ------------------------------------------------
    // ------------------------------------------------

    RESULT.reference()
  }

  // ------------------------------------------------------------
  // ------------------------------------------------------------

  def prepare(array:SciArray, label:String) : SciArray =
  {
    array
      .redimension(
        List(GDS.sid_D, GDS.enumeration_D, GDS.chr_D),
        List(GDS.left_D.toAttribute(false), GDS.right_D.toAttribute(false), GDS.strand_D.toAttribute(false)) ::: array.getAttributes())
      .cast(
        List(GDS.sid_D, GDS.enumeration_D, GDS.chr_D).map(d => d.label(label)),
        (List(GDS.left_A, GDS.right_A, GDS.strand_D.toAttribute()) ::: array.getAttributes()).map(a => a.label(label)))
  }


}

