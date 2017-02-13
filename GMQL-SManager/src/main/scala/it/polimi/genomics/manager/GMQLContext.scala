package it.polimi.genomics.manager

import it.polimi.genomics.core
import it.polimi.genomics.core.{BinSize, ImplementationPlatform}
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
  * @param implPlatform {@link core.ImplementationPlatform} which set the platform type; Spark, Flink, or SciDB.
  * @param gMQLRepository {@link GMQLRepository}, set the type of GMQL repository; local, remote, or HDFS.
  * @param outputFormat {@link core.GMQLOutputFormat} sets the output format of the job. GTF or TAB.
  * @param binSize {@link BinSize}, sets the detaults bin size for the genometric operations.
  * @param username {@link String} of the user name.
  * @param sc {@link SparkContext}
  */
case class GMQLContext(val implPlatform: core.ImplementationPlatform.Value,val gMQLRepository: GMQLRepository,val  outputFormat: core.GMQLOutputFormat.Value, val binSize:BinSize = BinSize(),val username:String = repo_Utilities().USERNAME, sc:SparkContext = null) {
  sc.setLogLevel("INFO")

  /**
    * default constructor
    */
  def this() = this(core.ImplementationPlatform.SPARK,new LFSRepository(),core.GMQLOutputFormat.TAB)
  def this(gMQLRepository: GMQLRepository) = this(core.ImplementationPlatform.SPARK,gMQLRepository,core.GMQLOutputFormat.TAB)
  def this(gMQLRepository: GMQLRepository,outputFormat: core.GMQLOutputFormat.Value) = this(core.ImplementationPlatform.SPARK,gMQLRepository,outputFormat)


  val implementation = if(implPlatform == ImplementationPlatform.SPARK){
    new GMQLSparkExecutor(binSize = binSize, sc=sc  , outputFormat = outputFormat)
  }else /*if(implPlatform == ImplementationPlatform.FLINK)*/{
    new FlinkImplementation( binSize=binSize, outputFormat = outputFormat)
  }
}