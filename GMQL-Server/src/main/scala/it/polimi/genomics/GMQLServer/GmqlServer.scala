package it.polimi.genomics.GMQLServer

import it.polimi.genomics.GMQLServer.optimization.{DefaultOptimizer, GMQLOptimizer, MetaFirstOptimizer}
import it.polimi.genomics.core.GMQLLoader
import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
import it.polimi.genomics.core.DataStructures._
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.{GMQLLoader, GRecordKey, GValue}

import scala.collection.mutable

/**
  * The manager of the reading, materialization and optimization of the IRVariable.
  *
  * @param implementation : the implementation that we want to use
  * @param binning_size   : parameter of the binning algorithm
  */
class GmqlServer(var implementation: Implementation, binning_size: Option[Long] = None) {

  implicit val binning_parameter = BinningParameter(binning_size)
  var meta_output_path: Option[String] = None
  var region_output_path: Option[String] = None
  var materializationList: mutable.MutableList[IRVariable] = mutable.MutableList()

  var gmqlOptimizer: GMQLOptimizer = new DefaultOptimizer

  def run(graph: Boolean = false) = {
    implementation.to_be_materialized ++= optimise(materializationList.toList)
    implementation.go()
  }

  def setOptimizer(optimizer: GMQLOptimizer): Unit = {
    this.gmqlOptimizer = optimizer
  }

  def setMetaPath(path: String) = {
    meta_output_path = Some(path)
    this
  }

  def setRegionPath(path: String) = {
    region_output_path = Some(path)
    this
  }

  def setOutputPath(path: String) = {
    region_output_path = Some(path)
    meta_output_path = Some(path)
    this
  }

  def clearMaterializationList(): Unit = {
    this.materializationList.clear()
  }

  def COLLECT(iRVariable: IRVariable): Any = {
    implementation.collect(optimise(List(iRVariable)).head)
  }

  def COLLECT_ITERATOR(iRVariable: IRVariable): (Iterator[(GRecordKey, Array[GValue])], Iterator[(Long, (String, String))], List[(String, PARSING_TYPE)]) = {
    implementation.collectIterator(optimise(List(iRVariable)).head)
  }

  def TAKE_FIRST(iRVariable: IRVariable, n: Int): (Array[(GRecordKey, Array[GValue])], Array[(Long, (String, String))], List[(String, PARSING_TYPE)]) = {
    implementation.takeFirst(optimise(List(iRVariable)).head, n)
  }


  def TAKE(iRVariable: IRVariable, n: Int): Any = {
    implementation.take(optimise(List(iRVariable)).head, n)
  }

  def MATERIALIZE(variable: IRVariable) = {

    if (!meta_output_path.isDefined) {
      println("Disgrace!! The metadata output path has not been specified yet!")
    }


    if (!region_output_path.isDefined) {
      println("Disgrace!! The region output path has not been specified yet!")
    }
    import scala.collection.JavaConverters._
    val dag_md = new IRStoreMD(meta_output_path.get, variable.metaDag, IRDataSet(meta_output_path.get, List[(String, PARSING_TYPE)]().asJava))
    val dag_rd = new IRStoreRD(region_output_path.get, variable.regionDag, variable.metaDag, variable.schema, IRDataSet(meta_output_path.get, List[(String, PARSING_TYPE)]().asJava))

    val new_var = new IRVariable(dag_md, dag_rd, variable.schema, dependencies = List(variable))
    materializationList += new_var
    //implementation.to_be_materialized += new_var
  }

  /**
    *
    * @param paths
    * @return UnfinishedREAD(path), to which the USING method must be applied
    */
  def READ(paths: List[String], location : GMQLInstance ): UnfinishedREAD = new UnfinishedREAD(paths, location)

  def READ(paths: List[String]): UnfinishedREAD = new UnfinishedREAD(paths)


  /**
    *
    * @param paths
    * @return UnfinishedREAD(path), to which the USING method must be applied
    */
  def READ(paths: String, location : GMQLInstance ): UnfinishedREAD = new UnfinishedREAD(List(paths), location)

  def READ(paths: String): UnfinishedREAD = new UnfinishedREAD(List(paths))


  /**
    * Class used to generate a variable reading from the file system
    *
    * @param paths The list of paths where the dataset is stored. It must contains two sub-directory named "meta" and "files"
    */
  class UnfinishedREAD(paths: List[String], location : GMQLInstance = LOCAL_INSTANCE) {
    /**
      * Used to build a new IRVariable
      *
      * @param loader a class containing the region and the metadata parser
      * @tparam IR Input type of region data
      * @tparam OR Output type of region data
      * @tparam IM Input type of meta data
      * @tparam OM Output type of metadata
      * @return a new IRVariable
      */

    def USING[IR, OR, IM, OM](loader: GMQLLoader[IR, OR, IM, OM]) = {

      val ds = implementation.getDataset(paths.head).get

      val dagMD = IRReadMD(paths, loader, ds)
      dagMD.addAnnotation(EXECUTED_ON(location))
      val dagRD = IRReadRD(paths, loader, ds)
      dagRD.addAnnotation(EXECUTED_ON(location))

      import scala.collection.JavaConverters._
      val schema = if (ds.schema.isEmpty)
        loader.schema
      else
        ds.schema.asScala.toList

      IRVariable(dagMD, dagRD, schema)
    }

    def USING(metaDS: Any, regionDS: Any, sch: List[(String, PARSING_TYPE)]) = {

      val dagMD = IRReadMEMMD(metaDS)
      val dagRD = IRReadMEMRD(regionDS)


      IRVariable(dagMD, dagRD, sch)
    }

  }

  def readFile[IR, OR, IM, OM](path: String, loader: GMQLLoader[IR, OR, IM, OM]): IRVariable = {
    val ds = implementation.getDataset(path).get
    val dagRD = IRReadFileRD(path, loader, ds)
    val dagMD = IRGenerateMD(dagRD)

    import scala.collection.JavaConverters._
    val schema = if (ds.schema.isEmpty)
      loader.schema
    else
      ds.schema.asScala.toList
    IRVariable(dagMD, dagRD, schema)
  }



  /**
    * Optimizes the DAG (materialization list) using the specified optimizer in GMQLServer
    *
    * @param materializationList : list of IRVariable
    * @return the optimized List of IRVariable
    */
  def optimise(materializationList: List[IRVariable]): List[IRVariable] = {
    this.gmqlOptimizer.optimize(materializationList)
  }

}

