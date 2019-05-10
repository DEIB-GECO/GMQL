package it.polimi.genomics.GMQLServer

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import it.polimi.genomics.core.{GMQLLoaderBase, GRecordKey, GValue}
import it.polimi.genomics.core.DataStructures.Builtin._
import it.polimi.genomics.core.DataStructures.GroupMDParameters.{NoTop, Top, TopG}
import it.polimi.genomics.core.DataStructures.GroupRDParameters.FIELD
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToRegion
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.ParsingType._

import scala.collection.mutable
import scala.collection.mutable.HashSet

/**
 * Handle the logic of the GMQL engine.
 * The workflow is: the user request a new variable to the server. Then, the user modify the variable using variable's methods.
 * The user upload to the server the result variable. Finally, the user invokes the go() method and the computation starts.
 */
abstract class Implementation {

  /** Returns the factory that builds metadata modifiers */
  def metaExtensionFactory: MetaExtensionFactory = DefaultMetaExtensionFactory

  /** Returns the factory that builds metadata modifiers */
  def metaAggregateFunctionFactory: MetaAggregateFactory = DefaultMetaAggregateFactory

  /** Returns the factory that builds region modifiers */
  def regionExtensionFactory:RegionExtensionFactory = DefaultRegionExtensionFactory

  /**
   * Needed for the GMQL translation, returns the factory that generates build-in gmql map functions
   * It's implementation can be extended by an execution engine.
   */
  def mapFunctionFactory:MapFunctionFactory = DefaultRegionsToRegionFactory

  /**
   * Needed for the GMQL translation, returns the factory that generates build-in gmql extend functions
   * It's implementation can be extended by an execution engine.
   */
  def extendFunctionFactory:ExtendFunctionFactory = DefaultRegionsToMetaFactory

  /** List of the variables to be materialized.
    * The user has to add to this list all the variables he wants to materialize. */
  val to_be_materialized : mutable.MutableList[IRVariable] = mutable.MutableList()

  /** Starts the execution */
  def go()

  def collect(iRVariable: IRVariable):Any

  def collectIterator(iRVariable: IRVariable): (Iterator[(GRecordKey, Array[GValue])], Iterator[(Long, (String, String))], List[(String, PARSING_TYPE)])

  def take(iRVariable: IRVariable, n:Int):Any

  def takeFirst(iRVariable: IRVariable, n:Int): (Array[(GRecordKey, Array[GValue])], Array[(Long, (String, String))], List[(String, PARSING_TYPE)])

  /** stop GMQL implementation (kill a job)*/
  def stop()

  /** given the name of a parser, returns it. It must at least provide a parser for the "default" name */
  def getParser(name : String,dataset:String) : GMQLLoaderBase

  /** */
  def getDataset(identifier : String) : Option[IRDataSet] = {
    import scala.collection.JavaConverters._
    Some(IRDataSet(identifier, List[(String,PARSING_TYPE)]().asJava))
  }
}
