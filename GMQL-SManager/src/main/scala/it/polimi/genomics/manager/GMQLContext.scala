package it.polimi.genomics.manager

import it.polimi.genomics.GMQLServer.Implementation
import it.polimi.genomics.core
import it.polimi.genomics.core.GDMSUserClass.GDMSUserClass
import it.polimi.genomics.core.{BinSize, GDMSUserClass, GMQLSchemaCoordinateSystem, ImplementationPlatform}
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.repository.FSRepository.LFSRepository
import it.polimi.genomics.repository.{GMQLRepository, Utilities => repo_Utilities}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import org.apache.spark.SparkContext

/**
  * Created by abdulrahman on 23/01/2017.
  *
  * GMQL context, includes the executing platform,
  * the repository,
  * the outputformat,
  * and the binning size for Genometric operations
  *
  * @param implPlatform { @link core.ImplementationPlatform} which set the platform type; Spark, Flink, or SciDB.
  * @param gMQLRepository { @link GMQLRepository}, set the type of GMQL repository; local, remote, or HDFS.
  * @param outputFormat { @link core.GMQLSchemaFormat} sets the output format of the job. GTF or TAB.
  * @param binSize { @link BinSize}, sets the detaults bin size for the genometric operations.
  * @param username { @link String} of the user name.
  * @param sc { @link SparkContext}
  */
case class GMQLContext(val implPlatform: core.ImplementationPlatform.Value, val gMQLRepository: GMQLRepository, val outputFormat: core.GMQLSchemaFormat.Value, val outputCoordinateSystem: core.GMQLSchemaCoordinateSystem.Value = GMQLSchemaCoordinateSystem.Default, val binSize: BinSize = BinSize(), val username: String = repo_Utilities().USERNAME, val userClass:GDMSUserClass = GDMSUserClass.PUBLIC,  sc: SparkContext = null, checkQuota: Boolean = false) {
  try {
    sc.setLogLevel("WARN")
  } catch {
    case _ => println("Spark Context is not set..")
  }

  /**
    * default constructor
    */
  def this() = this(core.ImplementationPlatform.SPARK, new LFSRepository(), core.GMQLSchemaFormat.TAB, core.GMQLSchemaCoordinateSystem.Default)

  /**
    * Construct GMQL Context with the repository type
    *
    * @param gMQLRepository  one of [[GMQLRepository]] subclasses
    * @return [[GMQLContext]] instance
    */
  def this(gMQLRepository: GMQLRepository) = this(core.ImplementationPlatform.SPARK, gMQLRepository, core.GMQLSchemaFormat.TAB, core.GMQLSchemaCoordinateSystem.Default)

  /**
    * Construct GMQL Context with the repository type, and output format type
    *
    * @param gMQLRepository  one of [[GMQLRepository]] subclasses
    * @param outputFormat one of the [[it.polimi.genomics.core.GMQLSchemaFormat]] values.
    * @param outputCoordinateSystem one of the [[it.polimi.genomics.core.GMQLSchemaCoordinateSystem]] values
    * @return [[GMQLContext]] instance
    */
  def this(gMQLRepository: GMQLRepository, outputFormat: core.GMQLSchemaFormat.Value, outputCoordinateSystem: core.GMQLSchemaCoordinateSystem.Value) = this(core.ImplementationPlatform.SPARK, gMQLRepository, outputFormat, outputCoordinateSystem)


  /**
    * the implementation instance as the executor that will run GMQL script.
    */
  val implementation: Implementation = if (implPlatform == ImplementationPlatform.SPARK) {
    new GMQLSparkExecutor(binSize = binSize, sc = sc, outputFormat = outputFormat, outputCoordinateSystem = outputCoordinateSystem)
  } else /*if(implPlatform == ImplementationPlatform.FLINK)*/ {
    new FlinkImplementation(binSize = binSize, outputFormat = outputFormat)
  }
}
