package it.polimi.genomics

import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.ParsingType._

import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.scidb.utility.StringUtils
import it.polimi.genomics.scidbapi.schema.{DataType, Attribute}


/**
  * Provides some utility used inside the package
  */
package object scidb
{

  // ------------------------------------------------------------
  // -- DAG -----------------------------------------------------

  /**
    * This class provides some methods to serialize
    * the dags node
    *
    * @param dag
    */
  implicit class DAGtoString(dag:IROperator)
  {
    /**
      * Convert a dag note into a string to visualize it
      *
      * @return
      */
    def dagToString : String =
    {
      dag match {

        // Meta Operators -------------------------------

        case IRCombineMD(metajoin, anchor, experiment, leftName, rightName) =>
          "IRCombineMD ( " +
            "\n\tintermediate: " + (if(dag.intermediateResult.isDefined) dag.intermediateResult.get.toString else "None") +
            "\n\n\tmetajoin: " + (metajoin match {
            case SomeMetaJoinOperator(operator) => StringUtils.tabf(operator.dagToString)
            case NoMetaJoinOperator(operator) => StringUtils.tabf(operator.dagToString)
          }) +
            "\n\n\tanchor: "+ leftName +" - " + StringUtils.tabf(anchor.dagToString) +
            "\n\n\texperiment: "+ rightName +" - " + StringUtils.tabf(experiment.dagToString) +
            "\n)"

        case IRReadMD(_,_,ds) =>
          "IRReadMD ( " +
            "\n\tdataset: "+ ds +
            "\n\tintermediate: " + (if(dag.intermediateResult.isDefined) dag.intermediateResult.get.toString else "None") +
            " )"

        case IRStoreMD(_, child,ds) =>
          "IRStoreMD ( " +
            "\n\tdataset: "+ ds +
            "\n\tintermediate: " + (if(dag.intermediateResult.isDefined) dag.intermediateResult.get.toString else "None") +
            "\n\n\tchild: " + StringUtils.tabf(child.dagToString) +
            "\n\n)"

        case IRSelectMD(cond, child) =>
          "IRSelectMD ( " +
            "\n\tconditions: "+ cond +
            "\n\tintermediate: " + (if(dag.intermediateResult.isDefined) dag.intermediateResult.get.toString else "None") +
            "\n\n\tchild: " + StringUtils.tabf(child.dagToString) +
            "\n\n)"


        // Region Operators -----------------------------

        case IRGenometricJoin(metajoin, predicates, generator, _, anchor, experiment) =>
          "IRGenometricJoin ( " +
            "\n\tintermediate: " + (if(dag.intermediateResult.isDefined) dag.intermediateResult.get.toString else "None") +
            "\n\tpredicates: " + predicates +
            "\n\tgenerator: " + generator +
            "\n\n\tmetajoin: " + (metajoin match {
            case SomeMetaJoinOperator(operator) => StringUtils.tabf(operator.dagToString)
            case NoMetaJoinOperator(operator) => StringUtils.tabf(operator.dagToString)
          }) +
            "\n\n\tanchor: " + StringUtils.tabf(anchor.dagToString) +
            "\n\n\texperiment: " + StringUtils.tabf(experiment.dagToString) +
            "\n)"

        case IRGenometricMap(metajoin, aggrs, reference, experiment) =>
          "IRGenometricMap ( " +
            "\n\tintermediate: " + (if(dag.intermediateResult.isDefined) dag.intermediateResult.get.toString else "None") +
            "\n\taggregates: " + aggrs +
            "\n\n\tmetajoin: " + (metajoin match {
            case SomeMetaJoinOperator(operator) => StringUtils.tabf(operator.dagToString)
            case NoMetaJoinOperator(operator) => StringUtils.tabf(operator.dagToString)
          }) +
            "\n\n\tanchor: " + StringUtils.tabf(reference.dagToString) +
            "\n\n\texperiment: " + StringUtils.tabf(experiment.dagToString) +
            "\n)"

        case IRReadRD(_,_,ds) =>
          "IRReadRD ( " +
            "\n\tdataset: "+ ds +
            "\n\tintermediate: " + (if(dag.intermediateResult.isDefined) dag.intermediateResult.get.toString else "None") +
            " )"

        case IRStoreRD(_,child,d_,_,ds) =>
          "IRStoreRD ( " +
            "\n\tdataset: "+ ds +
            "\n\tintermediate: " + (if(dag.intermediateResult.isDefined) dag.intermediateResult.get.toString else "None") +
            "\n\n\tchild: " + StringUtils.tabf(child.dagToString) +
            "\n\n)"

        case IRSelectRD(cond, meta, child) =>
          "IRSelectRD ( " +
            "\n\tconditions: "+ cond +
            "\n\tmeta: "+ meta +
            "\n\tintermediate: " + (if(dag.intermediateResult.isDefined) dag.intermediateResult.get.toString else "None") +
            "\n\n\tchild: " + StringUtils.tabf(child.dagToString) +
            "\n\n)"

        // Region Operators -----------------------------

        case IRJoinBy(cond, anchor, experiment) =>
          "IRJoinBy ( " +
            "\n\tconditions: "+ cond +
            "\n\tintermediate: " + (if(dag.intermediateResult.isDefined) dag.intermediateResult.get.toString else "None") +
            "\n\n\tanchor: " + StringUtils.tabf(anchor.dagToString) +
            "\n\n\texperiment: " + StringUtils.tabf(experiment.dagToString) +
            "\n)"

        // Default --------------------------------------

        case _ => "child"
      }
    }
  }

}
