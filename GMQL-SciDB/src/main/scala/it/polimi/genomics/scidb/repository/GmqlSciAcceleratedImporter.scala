package it.polimi.genomics.scidb.repository

import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.scidb.GmqlSciConfig
import it.polimi.genomics.scidb.exception.IOErrorGmqlSciException
import it.polimi.genomics.scidb.libraries.gmql4scidb
import it.polimi.genomics.scidb.operators.GDS
import it.polimi.genomics.scidb.utility.SchemaUtils
import it.polimi.genomics.scidbapi.aggregate.AGGR_COUNT
import it.polimi.genomics.scidbapi.expression._
import it.polimi.genomics.scidbapi.schema.Attribute
import it.polimi.genomics.scidbapi.schema.DataType._
import it.polimi.genomics.scidbapi.script.SciCommandType._
import it.polimi.genomics.scidbapi.script.{SciCreate, SciCommand, SciOperation, SciScript}
import it.polimi.genomics.scidbapi.{SciAbstractArray, SciArray, SciStoredArray}

/**
  * Created by Cattani Simone on 25/05/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object GmqlSciAcceleratedImporter
{

  // ------------------------------------------------------------
  // -- IMPORT --------------------------------------------------

  /**
    * Provides the method to import a dataset into SciDB from a
    * local directory on the server
    *
    * @param name dataset name
    * @param path input files path
    * @param columns dataset schema
    * @param format input files format
    * @param complement name of complementation method
    */
  def importDS(end:Int,
               name:String,
               path:String,
               columns:List[(String, PARSING_TYPE, Int)],
               format:String = "tsv",
               complement:Option[String] = None)
  : Unit =
  {
    // ------------------------------------------------
    // Args control -----------------------------------

    if( !List("tsv","csv").contains(format) )
      throw new IOErrorGmqlSciException("Invalid file format, '"+ format +"' not supported")

    val schema = columns.map(item => (SchemaUtils.toAttribute(item._1, item._2), item._3))



    // ------------------------------------------------
    // Scan samples -----------------------------------

    val directory = GmqlSciFileSystem.ls(path)

    val samples = directory
      .map(_.getName)                                   // consider the file names inside the directory
      .filter(!_.endsWith(".meta"))                     // keep all the files that are not metadata

      .filter(item => directory                         // consider only files that have metadata, too
      .map(_.getName)
      .contains(item+".meta"))

      .zipWithIndex.map(item => (item._1,item._2+1))    // compute the sample id on for each file
      .slice(end-50,end)

    // ------------------------------------------------
    // ------------------------------------------------

    val script = new SciScript
    script.addStatement(new SciCommand(set_no_fetch))
    script.openQueue()

    // ------------------------------------------------
    // Import metadata --------------------------------

    val MD = importMD(name, path, samples, format, script)

    // ------------------------------------------------
    // Import regiondata ------------------------------

    val RD = importRD(name, path, samples, schema, format, complement, script)

    // ------------------------------------------------
    // Execution --------------------------------------

    if( GmqlSciConfig.scidb_server_debug )
      script.export("../output/","import.afl")

    if( GmqlSciConfig.scidb_server_on )
      script.run(
        GmqlSciConfig.scidb_server_ip,
        GmqlSciConfig.scidb_server_username,
        GmqlSciConfig.scidb_server_password,
        GmqlSciConfig.scidb_server_runtime_dir
      )

  }



  // ------------------------------------------------------------
  // -- IMPORT METADATA -----------------------------------------

  /**
    * Builds the metadata array imported by the input files
    *
    * @param name dataset name
    * @param path input files path
    * @param samples list of samples in the path
    * @param format input files format
    * @return the SciArray containing the imported metadata
    */
  def importMD(name:String,
               path:String,
               samples:List[(String,Int)],
               format:String = "tsv",
               script: SciScript)
  : (SciStoredArray) =
  {
    // ------------------------------------------------
    // Import raw tmp data ----------------------------

    val MD_RAW = samples
      .map(item => (
        SciArray.input(
          path +"/"+ item._1 +".meta",
          List(GDS.enumeration_D),
          List(GDS.name_A, GDS.value_A),
          -2, format
        ),
        item._2
        ))
      .map(item => (item._1.apply((A(GDS.sid_D.name),V(item._2))), item._2))
      .map(item => item._1
        .redimension(
          List(GDS.enumeration_D, GDS.sid_D),
          List(GDS.name_A, GDS.value_A)
        )
      )

    // ------------------------------------------------
    // ------------------------------------------------

    val MD_TMP = new SciStoredArray(
      MD_RAW(0).getDimensions(),
      MD_RAW(0).getAttributes(),
      name+"_MD_TMP",
      name+"_MD_TMP"
    )

    script.addStatement(SciCreate(true, MD_TMP.getAnchor(), MD_TMP.getDimensions(), MD_TMP.getAttributes()))


    // ------------------------------------------------
    // Import raw data --------------------------------

    MD_RAW.map(a => script.addStatement(a.insert(MD_TMP)))

    // ------------------------------------------------
    // Elaborate data ---------------------------------

    val MD_hashed = MD_TMP.reference().apply(
      (A(GDS.nid1_D.name), LIB(gmql4scidb.hash, GDS.name_A.e)),
      (A(GDS.nid2_D.name), LIB(gmql4scidb.hash, OP(GDS.name_A.e,"+",C_STRING(V("0"))))),
      (A(GDS.vid1_D.name), LIB(gmql4scidb.hash, GDS.value_A.e)),
      (A(GDS.vid2_D.name), LIB(gmql4scidb.hash, OP(GDS.value_A.e,"+",C_STRING(V("0")))))
    )

    val MD = MD_hashed
      .redimension(
        GDS.meta_dimensions_D,
        GDS.meta_attributes_A
      )
      .store(name + "_MD")

    // ------------------------------------------------
    // Result -----------------------------------------

    script.addStatement(MD)
    script.addStatement(new SciOperation("remove", MD_TMP.getAnchor()))

    MD
  }



  // ------------------------------------------------------------
  // -- IMPORT REGIONDATA ---------------------------------------

  /**
    * Builds the region data array imported by the input files
    *
    * @param name dataset name
    * @param path input files path
    * @param samples list of samples in the path
    * @param schema input schema (related to the file)
    * @param format input file format
    * @param complement eventual complementation procedure
    * @return a tuple containing the tree arraies necessary to build the result
    */
  def importRD(name:String,
               path:String,
               samples:List[(String,Int)],
               schema:List[(Attribute,Int)],
               format:String = "tsv",
               complement:Option[String] = None,
               script: SciScript)
  : (SciStoredArray) =
  {
    // ------------------------------------------------
    // Prepare schemas --------------------------------

    // File schema ------------------------------------
    val fileSchema = List
      .tabulate( schema.maxBy(_._2)._2 )( n =>                  // produce the required number of attirbutes
        schema.find(item => item._2 == n+1) match
        {
          case Some((attr,_)) => attr                           // if the attribute is required fetch it
          case None => new Attribute("__synth"+(n+1), STRING)   // otherwise produce a synthetic one
        }
      )
      .map(item => item.name match {                            // modifies names to be compatible with application
        case GDS.chr_D.name => GDS.CHR_A
        case GDS.left_D.name => GDS.left_A
        case GDS.right_D.name => GDS.right_A
        case GDS.strand_D.name => GDS.STRAND_A
        case _ => item
      })

    // Implicit schema --------------------------------
    val implicitSchema = fileSchema                              // defines the implicit schema
      .filter(attr =>
      ! GDS.import_dimensions_A.contains(attr) &&
        ! attr.name.startsWith("__synth")
    )



    // ------------------------------------------------
    // Import prepared data ---------------------------

    val RD_RAW = samples
      .map(item => (
        SciArray.input(
          path +"/"+ item._1,
          List(GDS.enumeration_D),
          fileSchema, -2, format
        ),
        item._2
        ))
      .map(item => (item._1.apply((A(GDS.sid_D.name),V(item._2))), item._2))
      .map(item => complement match {
        case Some(code) if(code == "bed") => (item._1.apply((GDS.STRAND_A.e, V("*"))), item._2)
        case None => (item._1, item._2)
      })
      .map(item => item._1
        .redimension(
          List(GDS.enumeration_D, GDS.sid_D),
          GDS.import_dimensions_A ::: implicitSchema
        )
      )

    // ------------------------------------------------
    // ------------------------------------------------

    val RD_PREPARED = new SciStoredArray(
      RD_RAW(0).getDimensions(),
      RD_RAW(0).getAttributes(),
      name+"_RD_PREPARED",
      name+"_RD_PREPARED"
    )

    script.addStatement(SciCreate(true, RD_PREPARED.getAnchor(), RD_PREPARED.getDimensions(), RD_PREPARED.getAttributes()))

    // ------------------------------------------------
    // Import prepared data ---------------------------

    RD_RAW.map(a => script.addStatement(a.insert(RD_PREPARED)))

    // ------------------------------------------------
    // Add new chromosomes names ----------------------

    val RD_CHR = new SciStoredArray(
      List(GDS.chri_D),
      List(GDS.CHR_A),
      "RD_CHR",
      "RD_CHR"
    )

    val new_CHR = RD_PREPARED.reference()
      .project(GDS.CHR_A.e)                                        // considers only the CHR attribute
      .sort()().uniq()                                                // considers only unique values of the imported array
      .index_lookup(RD_CHR.reference(), GDS.CHR_A.e, A("CHR_EX"))  // tries to apply an existing index value
      .filter(FUN("is_null", A("CHR_EX"))).sort()()                   // keeps only the not already existing values
      .cross_join(RD_CHR.reference().aggregate(AGGR_COUNT(A("*"), A("count")))())()
      .apply((A("CHR_NEW"), OP(D("n"),"+",C_INT64(A("count")))))               // define new index to be added to the current list
      .redimension(                                                   // reshape the result according to the RD_CHR
      List(GDS.chri_D.rename("CHR_NEW")),List(GDS.CHR_A))
      .cast(                                                          // rename the obtained array
        List(GDS.chri_D),List(GDS.CHR_A))
      .insert(RD_CHR)

    script.addStatement(new_CHR)

    // ------------------------------------------------
    // Import final data ------------------------------

    val RD = RD_PREPARED.reference()
      .index_lookup(RD_CHR.reference(), GDS.CHR_A.e, A(GDS.chr_D.name))  // assign the chr index
      .apply((                                                              // convert strand into a integer value
      A(GDS.strand_D.name),
      IF( OP(GDS.STRAND_A.e,"=",V("*")), V(1), IF( OP(GDS.STRAND_A.e,"=",V("+")), V(2), V(0))) ))
      .redimension(                                                         // reshape as the final result
        GDS.regions_dimensions_D,
        implicitSchema
      )
      .store(name + "_RD")                                                  // store the final result

    // ------------------------------------------------
    // Result -----------------------------------------

    script.addStatement(RD)
    script.addStatement(new SciOperation("remove", RD_PREPARED.getAnchor()))

    RD
  }

}