package it.polimi.genomics.scidb.repository

import it.polimi.genomics.core.ParsingType._
import it.polimi.genomics.scidb.GmqlSciConfig
import it.polimi.genomics.scidb.exception.IOErrorGmqlSciException
import it.polimi.genomics.scidb.operators.GDS
import it.polimi.genomics.scidb.utility.SchemaUtils
import it.polimi.genomics.scidbapi.SortingKeyword._
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.script.SciCommandType._
import it.polimi.genomics.scidbapi.script.{SciCommand, SciScript}
import it.polimi.genomics.scidbapi.{SciAbstractArray, SciArray, SciStoredArray}


object GmqlSciExporter
{

  // ------------------------------------------------------------
  // -- EXPORT --------------------------------------------------

  /**
    * Provides the method to export a dataset to a local directory
    * into the coordinator server
    *
    * @param name dataset name
    * @param path output directory
    * @param columns dataset schema
    * @param format input files format
    */
  def exportDS(name:String,
               path:String,
               columns:java.util.List[(String, PARSING_TYPE)],
               format:String = "tsv")
    : Unit =
  {
    // ------------------------------------------------
    // Args control -----------------------------------

    if( !List("tsv","csv").contains(format) )
      throw new IOErrorGmqlSciException("Invalid file format, '"+ format +"' not supported")
    import scala.collection.JavaConverters._
    val schema = columns.asScala.map(item => SchemaUtils.toAttribute(item._1, item._2)).toList

    // ------------------------------------------------
    // Directory preparation --------------------------

    GmqlSciFileSystem.mkdir(path)

    // ------------------------------------------------
    // Array fetch ------------------------------------

    val MD = new SciArray(
      GDS.meta_dimensions_D,
      GDS.meta_attributes_A,
      name +"_MD"
    )

    val RD = new SciArray(
      GDS.regions_dimensions_D,
      schema,
      name +"_RD"
    )

    // ------------------------------------------------
    // Samples list -----------------------------------

    val samples = sids(MD, path)

    // ------------------------------------------------
    // Export metadata --------------------------------

    val metadata = exportMD(name, path, MD, samples, format)

    // ------------------------------------------------
    // Export regiondata ------------------------------

    val regiondata = exportRD(name, path, RD, samples, format)

    // ------------------------------------------------
    // Execution --------------------------------------

    val script = new SciScript
    script.addStatement(new SciCommand(set_no_fetch))

    metadata.map(item => script.addStatement(item))
    regiondata.map(item => script.addStatement(item))

    if( GmqlSciConfig.scidb_server_debug )
      script.export("../output/","export.afl")

    if( GmqlSciConfig.scidb_server_on )
      script.run(
        GmqlSciConfig.scidb_server_ip,
        GmqlSciConfig.scidb_server_username,
        GmqlSciConfig.scidb_server_password,
        GmqlSciConfig.scidb_server_runtime_dir
      )

  }



  // ------------------------------------------------------------
  // -- LIST SAMPLES --------------------------------------------

  /**
    * Return the list of the samples list for an array
    *
    * @param MD source array
    * @param path output path
    * @return the list of sample ids
    */
  def sids(MD:SciArray,
           path:String)
    : List[Long] =
  {
    // Prepare query ----------------------------------

    val MDSamples = MD
      .apply((A("sample"), GDS.sid_D.e))
      .project(A("sample"))
      .sort()().uniq()
      .save(path+"/_samples.csv", -2, "csv")

    // Run query --------------------------------------

    val script = new SciScript
    script.addStatement(new SciCommand(set_no_fetch))
    script.addStatement(MDSamples)

    if( GmqlSciConfig.scidb_server_on )
      script.run(
        GmqlSciConfig.scidb_server_ip,
        GmqlSciConfig.scidb_server_username,
        GmqlSciConfig.scidb_server_password,
        GmqlSciConfig.scidb_server_runtime_dir
      )

    // List extraction --------------------------------

    val list = GmqlSciFileSystem.cat(path+"/_samples.csv")
      .getLines().toList
      .diff(List("sample"))
      .map(_.toLong)

    // Clean file -------------------------------------

    GmqlSciFileSystem.rm(path+"/_samples.csv")

    // Result -----------------------------------------

    list
  }



  // ------------------------------------------------------------
  // -- EXPORT METADATA -----------------------------------------

  /**
    * Exports the metadata array into a list of files, one for each
    * sample in the dataset
    *
    * @param name dataset name
    * @param path output files path
    * @param MD source array to be exported
    * @param samples list of samples id
    * @param format output files format
    * @return the list of scidb commands
    */
  def exportMD(name:String,
               path:String,
               MD:SciArray,
               samples:List[Long],
               format:String = "tsv")
    : List[SciStoredArray] =
  {
    val digits = samples.max.toString.length

    // Prepare query ----------------------------------

    val MD_exported = samples
      .map(item => (MD.filter(OP(GDS.sid_D.e, "=", V(item))), item))                                 // obtain metadata for each sample
      .map(item => (item._1.sort((GDS.name_A.e, asc), (GDS.value_A.e, asc))(), item._2))          // reshape the result to be ready to be saved
      .map(item => item._1.save(                                                                        // save the sample metadata
        path +"/"+ name +"_"+ ("%0"+digits+"d").format((item._2)) +".meta",
        -2, format
      ))

    // Return query -----------------------------------

    MD_exported
  }



  // ------------------------------------------------------------
  // -- IMPORT REGIONDATA ---------------------------------------

  /**
    * Exports the regiondata array into a list of files, one for each
    * sample in the dataset
    *
    * @param name dataset name
    * @param path output files path
    * @param RD source array to be exported
    * @param samples list of samples id
    * @param format output files format
    * @return the list of scidb commands
    */
  def exportRD(name:String,
               path:String,
               RD:SciArray,
               samples:List[Long],
               format:String = "tsv")
    : List[SciAbstractArray] =
  {

    val digits = samples.max.toString.length

    val RD_CHR = new SciStoredArray(
      List(GDS.chri_D),
      List(GDS.CHR_A),
      "RD_CHR",
      "RD_CHR"
    )

    // Prepare query ----------------------------------

    val RD_exported = samples
      .map(item => (RD.filter(OP(GDS.sid_D.e, "=", V(item))), item))                                 // obtain regiondata for each sample

      .map(item => (item._1.cross_join(                                                                 // obtain the chr char
        RD_CHR.reference(),
        "DATASET", "CHR"
      )(
        (GDS.chr_D.alias("DATASET").e, GDS.chri_D.alias("CHR").e)
      ), item._2))

      .map(item => (item._1.apply((                                                                     // obtain the strand char
        GDS.STRAND_A.e,
        IF( OP(GDS.strand_D.e,"=",V(1)), V("*"), IF( OP(GDS.strand_D.e,"=",V(2)), V("+"), V("-")))
      )), item._2))

      .map(item => (item._1.apply(                                                                      // apply the left and right values
        (GDS.LEFT_A.e, GDS.left_D.e),
        (GDS.RIGHT_A.e, GDS.right_D.e)
      ), item._2))

      .map(item => (item._1.sort(                                                                       // reshape the result to be ready to be saved
        (GDS.CHR_A.e, asc),
        (GDS.LEFT_A.e, asc),
        (GDS.RIGHT_A.e, asc),
        (GDS.STRAND_A.e, asc)
      )(), item._2))

      .map(item => (item._1.redimension(                                                                // coordinates reposition
        item._1.getDimensions(),
        GDS.export_dimensions_A ::: RD.getAttributes()
      ), item._2))

      .map(item => item._1.save(                                                                        // save the sample regiondata
        path +"/"+ name +"_"+ ("%0"+digits+"d").format((item._2)),
        -2, format
      ))

    // Return query -----------------------------------

    RD_exported
  }


}
