package it.polimi.genomics.scidb

import it.polimi.genomics.GMQLServer.Implementation
import it.polimi.genomics.core.DataStructures.GroupRDParameters.FIELD
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.GMQLLoaderBase
import it.polimi.genomics.scidb.exception.InternalErrorGmqlSciException
import it.polimi.genomics.scidb.operators.metagroup.GmqlMetaGroupByMGD
import it.polimi.genomics.scidb.operators.metajoin.GmqlMetaJoinMJD
import it.polimi.genomics.scidb.operators.region._
import it.polimi.genomics.scidb.operators._
import it.polimi.genomics.scidb.operators.meta._
import it.polimi.genomics.scidb.repository.GmqlSciRepositoryManager
import it.polimi.genomics.scidb.utility.{JoinPredUtils, StringUtils}
import it.polimi.genomics.scidbapi.script.{SciCommand, SciScript}
import it.polimi.genomics.scidbapi.script.SciCommandType._


class GmqlSciImplementation
  extends Implementation
{

  // ------------------------------------------------------------
  // -- INFORMATIONS --------------------------------------------

  val repository = new GmqlSciRepositoryManager

  override def collect(iRVariable: IRVariable): Any = ???
  /**
    * This methods returns the dataset for the required position
    *
    * @param identifier dataset identifier
    * @return the required dataset
    */
  override def getDataset(identifier : String) : Option[IRDataSet] =
    repository.fetch(identifier)




  // ------------------------------------------------------------
  // -- MANAGEMENT ----------------------------------------------

  var dags : List[IRVariable] = List()

  /**
    * This methods add a new DAG to the execution list
    *
    * @param dag DAG to be added
    */
  def addDAG(dag:IRVariable) : Unit =
  {

    // ------------------------------------------------
    // append dag -------------------------------------

    dags = dags ::: List(dag)


    // ------------------------------------------------
    // prepare dag ------------------------------------

    println( "\n\n" +
      "\n-----------------------------------------------" +
      "\n-- SciDB PREPARATION --------------------------" +
      "\n-----------------------------------------------" +
      "\n"+ dag +
      "\n-----------------------------------------------" + "\n")

    prepare(dag.metaDag)
    prepare(dag.regionDag)

  }




  // ------------------------------------------------------------
  // -- IMPLEMENTATION ------------------------------------------

  /**
    * This method perform the execution of the query
    */
  def go() =
  {
    println( "\n\n" +
      "\n-----------------------------------------------" +
      "\n-- SciDB EXECUTION ----------------------------" +
      "\n-----------------------------------------------" + "\n" )

    // ------------------------------------------------
    // prepare script ---------------------------------

    val script = new SciScript
    if(!GmqlSciConfig.scidb_server_verbouse)
      script.addStatement(new SciCommand(set_no_fetch))
    script.addStatement(new SciCommand(set_timer))

    // ------------------------------------------------
    // compute dags -----------------------------------

    for( dag <- dags )
    {
      //println(dag.metaDag.dagToString)
      //println(dag.regionDag.dagToString)

      // ----------------------------------------------
      // compute dag ----------------------------------

      dag.metaDag.intermediateResult
        .get.asInstanceOf[GmqlOperator].compute(script)

      dag.regionDag.intermediateResult
        .get.asInstanceOf[GmqlOperator].compute(script)

    }

    // ------------------------------------------------
    // execute script ---------------------------------

    if( GmqlSciConfig.scidb_server_debug )
      script.export("../output/","run.afl")

    if( GmqlSciConfig.scidb_server_on )
      script.run(
        GmqlSciConfig.scidb_server_ip,
        GmqlSciConfig.scidb_server_username,
        GmqlSciConfig.scidb_server_password,
        GmqlSciConfig.scidb_server_runtime_dir
      )
  }



  override def stop() = ???



  // ------------------------------------------------------------
  // -- PREPARATION ---------------------------------------------

  /**
    * This method prepares the dag for the query execution
    *
    * @param node rood node to be prepared
    * @return
    */
  def prepare(node:IROperator) : GmqlOperator =
  {
    if( node.intermediateResult.isDefined ){

      // ------------------------------------------------
      // Add dependency ---------------------------------

      node.intermediateResult.get match
      {
        case ir:GmqlOperator => ir.use
        case _ => throw new InternalErrorGmqlSciException("Internal state is not a valid it.polimi.genomics.scidb.test.operator, node: '"+ node +"'")
      }

    }else{

      // ------------------------------------------------
      // Create new IR ----------------------------------

      val operator = node match
      {
        // -------------------------------------------------------------------------------------------------------------
        // Meta Operators ----------------------------------------------------------------------------------------------

        case IRAggregateRD(aggregates, regions) => new GmqlAggregateMD(prepare(regions).asInstanceOf[GmqlRegionOperator], aggregates)

        case IRCollapseMD(None, source) => new GmqlMergeMD(prepare(source).asInstanceOf[GmqlMetaOperator], None)
        case IRCollapseMD(Some(groupby), source) => new GmqlMergeMD(prepare(source).asInstanceOf[GmqlMetaOperator],
                                                                    Some(prepare(groupby).asInstanceOf[GmqlMetaGroupOperator]))

        case IRCombineMD(mj, anchor, experiment, aname, ename) => new GmqlCombineMD(prepare(mj.getOperator).asInstanceOf[GmqlMetaJoinOperator],
                                                                                    prepare(anchor).asInstanceOf[GmqlMetaOperator], aname,
                                                                                    prepare(experiment).asInstanceOf[GmqlMetaOperator], ename )

        case IRUnionAggMD(left, right, lname, rname) => new GmqlExtendMD( prepare(left).asInstanceOf[GmqlMetaOperator],
                                                                          prepare(right).asInstanceOf[GmqlMetaOperator],
                                                                          lname, rname)

        case IRGroupMD(conditions, aggrs, gname, md, rd) => new GmqlGroupMD(prepare(md).asInstanceOf[GmqlMetaOperator],
                                                                            prepare(rd).asInstanceOf[GmqlRegionOperator],
                                                                            conditions.attributes, gname, aggrs)

        case IRMergeMD(source, None) => new GmqlMergeMD(prepare(source).asInstanceOf[GmqlMetaOperator], None)
        case IRMergeMD(source, Some(groupby)) => new GmqlMergeMD( prepare(source).asInstanceOf[GmqlMetaOperator],
                                                                  Some(prepare(groupby).asInstanceOf[GmqlMetaGroupOperator]))

        case IROrderMD(ordering, destination, top, source) => new GmqlOrderMD(prepare(source).asInstanceOf[GmqlMetaOperator],
                                                                              ordering, top, destination)

        case IRProjectMD(attributes, _, source) => new GmqlProjectMD( prepare(source).asInstanceOf[GmqlMetaOperator],
                                                                      attributes.getOrElse(List()))

        case IRReadMD(_, _, ds) => new GmqlReadMD(ds)

        case IRSelectMD(conditions, source) => new GmqlSelectMD(prepare(source).asInstanceOf[GmqlMetaOperator], conditions)

        case IRSemiJoin(external, conditions, source) => new GmqlSemiJoinMD(prepare(source).asInstanceOf[GmqlMetaOperator],
                                                                            prepare(external).asInstanceOf[GmqlMetaOperator],
                                                                            conditions)

        case IRStoreMD(_, source, ds) => new GmqlStoreMD(ds, prepare(source).asInstanceOf[GmqlMetaOperator])

        case IRUnionMD(left, right, lname, rname) => new GmqlUnionMD( prepare(left).asInstanceOf[GmqlMetaOperator],
                                                                      prepare(right).asInstanceOf[GmqlMetaOperator],
                                                                      lname, rname)

        // -------------------------------------------------------------------------------------------------------------
        // Region Operators --------------------------------------------------------------------------------------------

        case IRDifferenceRD(mj, positive, negative,_) => new GmqlDifferenceRD(prepare(mj.getOperator).asInstanceOf[GmqlMetaJoinOperator],
                                                                            prepare(positive).asInstanceOf[GmqlRegionOperator],
                                                                            prepare(negative).asInstanceOf[GmqlRegionOperator])

        case IRRegionCover(flag, min, max, aggrs, None, source) => new GmqlGenometricCoverRD( prepare(source).asInstanceOf[GmqlRegionOperator],
                                                                                              None, flag, min, max, aggrs)
        case IRRegionCover(flag, min, max, aggrs, Some(groupby), source) => new GmqlGenometricCoverRD(prepare(source).asInstanceOf[GmqlRegionOperator],
                                                                                                      Some(prepare(groupby).asInstanceOf[GmqlMetaGroupOperator]),
                                                                                                      flag, min, max, aggrs)

        case IRGenometricJoin(mj, preds, builder, anchor, experiment) => new GmqlGenometricJoinRD(prepare(mj.getOperator).asInstanceOf[GmqlMetaJoinOperator],
                                                                                                  prepare(anchor).asInstanceOf[GmqlRegionOperator],
                                                                                                  prepare(experiment).asInstanceOf[GmqlRegionOperator],
                                                                                                  JoinPredUtils.convert(preds), builder)

        case IRGenometricMap(mj, aggrs, reference, experiment) => GmqlSciConfig.binning match
        {
          case 0 => new GmqlGenometricMapRD(prepare(mj.getOperator).asInstanceOf[GmqlMetaJoinOperator],
                                            prepare(reference).asInstanceOf[GmqlRegionOperator],
                                            prepare(experiment).asInstanceOf[GmqlRegionOperator],
                                            aggrs)

          case 1 => new GmqlGenometricMap1DBinRD( prepare(mj.getOperator).asInstanceOf[GmqlMetaJoinOperator],
                                                  prepare(reference).asInstanceOf[GmqlRegionOperator],
                                                  prepare(experiment).asInstanceOf[GmqlRegionOperator],
                                                  aggrs)

          case 2 => new GmqlGenometricMap2DBinRD( prepare(mj.getOperator).asInstanceOf[GmqlMetaJoinOperator],
                                                  prepare(reference).asInstanceOf[GmqlRegionOperator],
                                                  prepare(experiment).asInstanceOf[GmqlRegionOperator],
                                                  aggrs)
        }

        case IRGroupRD(conditions, aggrs, source) => new GmqlGroupRD( prepare(source).asInstanceOf[GmqlRegionOperator],
                                                                      conditions.getOrElse(List()).map(_ match { case FIELD(index) => index }),
                                                                      aggrs.getOrElse(List()))

        case IRMergeRD(source, None) => new GmqlMergeRD(prepare(source).asInstanceOf[GmqlRegionOperator], None)
        case IRMergeRD(source, Some(groupby)) => new GmqlMergeRD( prepare(source).asInstanceOf[GmqlRegionOperator],
                                                                  Some(prepare(groupby).asInstanceOf[GmqlMetaGroupOperator]))

        case IROrderRD(ordering, top, source) => new GmqlOrderRD( prepare(source).asInstanceOf[GmqlRegionOperator],
                                                                  ordering, top)

        case IRProjectRD(features, _, source) => new GmqlProjectRD( prepare(source).asInstanceOf[GmqlRegionOperator],
                                                                    features.getOrElse(List()))

        case IRPurgeRD(metadata, source) => new GmqlPurgeRD(prepare(source).asInstanceOf[GmqlRegionOperator],
                                                            prepare(metadata).asInstanceOf[GmqlMetaOperator])

        case IRReadRD(_, _, ds) => new GmqlReadRD(ds)

        case IRSelectRD(conditions, None, source) => new GmqlSelectRD( prepare(source).asInstanceOf[GmqlRegionOperator], None, conditions )
        case IRSelectRD(conditions, Some(metafilter), source) => new GmqlSelectRD(prepare(source).asInstanceOf[GmqlRegionOperator],
                                                                                  Some(prepare(metafilter).asInstanceOf[GmqlMetaOperator]),
                                                                                  conditions )

        case IRStoreRD(_, source, _,_,ds) => new GmqlStoreRD(ds, prepare(source).asInstanceOf[GmqlRegionOperator])

        case IRUnionRD(format, left, right) => new GmqlUnionRD( prepare(left).asInstanceOf[GmqlRegionOperator],
                                                                prepare(right).asInstanceOf[GmqlRegionOperator],
                                                                format)

        // -------------------------------------------------------------------------------------------------------------
        // Meta Join Operators -----------------------------------------------------------------------------------------

        case IRJoinBy(conditions, anchor, experiment) => new GmqlMetaJoinMJD( prepare(anchor).asInstanceOf[GmqlMetaOperator],
                                                                              prepare(experiment).asInstanceOf[GmqlMetaOperator],
                                                                              conditions )

        // -------------------------------------------------------------------------------------------------------------
        // Meta Join Operators -----------------------------------------------------------------------------------------

        case IRGroupBy(conditions, source) => new GmqlMetaGroupByMGD( prepare(source).asInstanceOf[GmqlMetaOperator],
                                                                      conditions)

        // -------------------------------------------------------------------------------------------------------------
        // Default -----------------------------------------------------------------------------------------------------
        case _ => throw new InternalErrorGmqlSciException("Operation not recognized, node:'"+ node +"'")
      }

      node.intermediateResult = Some(operator)

      operator
    }
  }






















  // ------------------------------------------------------------
  // -- ?????????????? ------------------------------------------

  def getParser(name:String, dataset:String) : GMQLLoaderBase = null            // TODO: da rimuovere dall'interfaccia

}
