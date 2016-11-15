package it.polimi.genomics.importer.ENCODEImporter

import java.io.{File, _}
import java.util
import java.util.zip.GZIPInputStream

import com.google.common.io.Files
import it.polimi.genomics.importer.FileLogger.FileLogger
import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION
import it.polimi.genomics.importer.GMQLImporter.{GMQLDataset, GMQLSource, GMQLTransformer}
import org.codehaus.jackson.map.MappingJsonFactory
import org.codehaus.jackson.{JsonNode, JsonParser, JsonToken}
import org.slf4j.LoggerFactory

import scala.xml.{Elem, XML}

/**
  * Created by Nacho on 10/13/16.
  * Object meant to be used for transform the data from ENCODE to data for GMQL,
  * files must be in the following format:
  *   - metadata file downloaded from ENCODE (1 single file for all the samples)
  *   - .gz data files downloaded from ENCODE.
  */
class ENCODETransformer extends GMQLTransformer {
  val logger = LoggerFactory.getLogger(this.getClass)

  //--------------------------------------------BASE CLASS SECTION---------------------------------------------
  /**
    * ENCODE data comes in .gz containers and metadata comes all together in a single file
    * splits the metadata file into many .meta files, and extracts the .gz containers.
    *
    * @param source contains specific download and sorting info.
    */
  override def transform(source: GMQLSource): Unit = {
    logger.info("Starting transformation for: " + source.outputFolder)
    source.datasets.foreach(dataset => {
      if(dataset.transformEnabled) {
        var logTransform = new FileLogger(
          source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Transformations")
        logTransform.markToCompare()
        logTransform.saveTable()

        val datasetOutputFolder = source.outputFolder + File.separator + dataset.outputFolder + File.separator
        val metadata = new File(datasetOutputFolder + "Downloads"+ File.separator + "metadata.tsv")
        if(metadata.exists()) {
          logger.info("Transformation for dataset: " + dataset.name)
          val folder = new File(datasetOutputFolder + "Transformations")
          if (!folder.exists()) {
            folder.mkdirs()
            logger.debug("Folder created: " + folder)
          }
          transformData(source, dataset)
          transformMeta(source, dataset)
        }
        else
          logger.warn("Transformation for dataset: " +dataset.name +" cannot be done. Metadata files have not been downloaded")

        logTransform = new FileLogger(
          source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Transformations")
        logTransform.markAsOutdated()
        logTransform.saveTable()

        val logDownload = new FileLogger(
          source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads")
        logDownload.markAsProcessed()
        logDownload.saveTable()
      }
    })
    organize(source)
  }
  //---------------------------------ENCODE DATA EXTRACTION/TRANSFORMATION SECTION---------------------------
  /**
    * Checks for which data has to be updated or added to the GMQL repository, and unGzips the needed files.
    * also saves in the Transform log, so when the Loader reads it,
    * knows if the data should be updated, deleted or added.
    *
    * @param source  contains specific download and sorting info.
    * @param dataset refers to the actual dataset being added
    */
  private def transformData(source: GMQLSource, dataset: GMQLDataset): Unit = {
    val logDownload = new FileLogger(
      source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads")
    val logTransform = new FileLogger(
      source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Transformations")
    logDownload.files.filter(_.name.endsWith(".gz")).foreach(file => {
      //this is to take out the ".gz"
      val name = file.name.substring(0, file.name.lastIndexOf("."))
      //should get file size, for the moment I pass the origin size just to have a value.
      if (logTransform.checkIfUpdate(name, file.name, file.originSize, file.lastUpdate)) {
        logger.debug("Start unGzipping: " + file.name)
        unGzipIt(
          source.outputFolder + File.separator + dataset.outputFolder +
            File.separator + "Downloads" + File.separator + file.name,
          source.outputFolder + File.separator + dataset.outputFolder +
            File.separator + "Transformations" + File.separator + name)
        logTransform.markAsUpdated(name)
        logger.info("UnGzipping: " + file.name + " DONE")
      }
    })
    logTransform.saveTable()
  }
  //---------------------------------------METADATA CREATION SECTION---------------------------------------------
  /**
    * deprecated
    * splits the metadata file into many .meta files as many rows the file has.
    *
    * @param source  contains specific download and sorting info.
    * @param dataset refers to the actual dataset being added
    */
  private def transformMeta(source: GMQLSource, dataset: GMQLDataset): Unit = {
    logger.info("Splitting ENCODE metadata for dataset: " + dataset.outputFolder)

    //fills the exclusion regexes into the list.
    source.parameters.filter(parameter => parameter._1.equalsIgnoreCase("encode_metadata_exclusion")).foreach(param =>{
      exclusionRegex.add(param._2)
    })

    val downloadPath = source.outputFolder + File.separator + dataset.outputFolder +
      File.separator + "Downloads"
    val transformationPath = source.outputFolder + File.separator + dataset.outputFolder +
      File.separator + "Transformations"

    val logDownloads = new FileLogger(downloadPath)

    val logTransformation = new FileLogger(transformationPath)

    logDownloads.files.filter(_.name.endsWith(".gz.json")).foreach(file => {
      val fileId = file.name.split('.').head
      val metadataName = file.name.replace(".gz.json", ".meta")
      val metadataDownloadPath = downloadPath + File.separator + file.name
      val transform = logTransformation.checkIfUpdate(
        metadataName,
        metadataDownloadPath,
        new File(metadataDownloadPath).getTotalSpace.toString,
        new File(metadataDownloadPath).lastModified().toString)
      if (transform) {
        logger.debug("Start metadata transformation: " + metadataName)
        val metadataDestinationPath = transformationPath+File.separator+metadataName
        transformMetaFromJson(metadataDownloadPath,metadataDestinationPath,fileId)
        logTransformation.markAsUpdated(metadataName)

        //here I replace the keys from the metadata.

        try {
          val config: Elem = XML.loadFile(source.parameters.filter(_._1.equalsIgnoreCase("metadata_replacement")).head._2)
          new it.polimi.genomics.importer.DefaultImporter.NULLTransformer().changeMetadataKeys(
            (config\\"metadata_replace_list"\"metadata_replace").map(replacement =>
              ((replacement\"regex").text,(replacement\"replace").text))
            ,metadataDestinationPath)
          logger.debug("File Created: " + metadataDestinationPath)
        }
        catch {
          case e: IOException => logger.error("not valid metadata replacement xml file: ")
        }
      }
    })
    logTransformation.saveTable()
  }
  //----------------------------------------METADATA FROM JSON SECTION-----------------------------------
  /**
    * by giving a .json file, it generates a .meta file with the json structure.
    * does an exception for the section "files" and needs the file id to achieve this.
    * @param metadataJsonFileName origin json file
    * @param metadataFileName destination .meta file
    * @param fileId id of the file being converted.
    */
  def transformMetaFromJson(metadataJsonFileName: String, metadataFileName: String, fileId: String): Unit ={
    val jsonFile = new File(metadataJsonFileName)
    if(jsonFile.exists()) {
      val f = new MappingJsonFactory()
      val jp: JsonParser = f.createJsonParser(jsonFile)

      val current: JsonToken = jp.nextToken()
      if (current != JsonToken.START_OBJECT) {
        logger.error("json root should be object: quiting. File: " + metadataJsonFileName)
      }
      else {
        val file = new File(metadataFileName)
        if(!file.exists()) {
          val writer = new PrintWriter(file)
          try {
            //this is the one that could throw an exception
            val node: JsonNode = jp.readValueAsTree()

            val metadataList = new java.util.ArrayList[String]()
            //here I handle the exceptions as "files" and "replicates"
            val replicateIds: List[String] = getReplicatesAndWriteFile(node, writer, fileId, metadataList)
            writeReplicates(node, writer, replicateIds, metadataList)
            //here is the regular case
            printTree(node, "", writer, metadataList)
          }
          catch {
            case e: IOException => logger.error("couldn't read the json tree: "+e.getMessage)
          }
          writer.close()
        }
        else
          logger.warn("metadata file not found file not found: "+metadataFileName)
      }
    }
    else
      logger.warn("Json file not found: "+metadataJsonFileName)
  }
  /**
    * handles the particular case of files, writes its metadata and returns a list with the replicates IDs used.
    * @param rootNode initial node of the json file.
    * @param writer output for metadata.
    * @param fileId id of the file that metadata is being extracted.
    * @param metaList list with already inserted meta to avoid duplication.
    * @return list with the replicates referred by the file.
    */
  def getReplicatesAndWriteFile(rootNode: JsonNode, writer: PrintWriter, fileId:String, metaList: java.util.ArrayList[String]): List[String] ={
    //particular cases first one is to find just the correct file to use its metadata.
    var replicates = List[String]()
    if(rootNode.has("files")){
      val files = rootNode.get("files").getElements
      while (files.hasNext) {
        val file = files.next()
        if (file.has("@id") && file.get("@id").asText().contains(fileId)) {
          if(file.has("biological_replicates")){
            val biologicalReplicates = file.get("biological_replicates")
            if(biologicalReplicates.isArray){
              val values = biologicalReplicates.getElements
              while(values.hasNext){
                val replicate = values.next()
                if(replicate.asText() != ""){
                  replicates = replicates :+ replicate.asText()
                }
              }
            }
          }
          //here is where the file is wrote
          printTree(file, "file", writer, metaList)
        }
      }
    }
    replicates
  }

  /**
    * handles the particular case of biological replicates, writes their metadata from a list of replicates
    * @param rootNode initial node of the json file.
    * @param writer output for metadata.
    * @param replicateIds list with the biological_replicate_number used by the file.
    * @param metaList list with already inserted meta to avoid duplication.
    */
  def writeReplicates(rootNode: JsonNode, writer: PrintWriter, replicateIds:List[String], metaList: java.util.ArrayList[String]): Unit ={
    if(rootNode.has("replicates")){
      val replicatesNode = rootNode.get("replicates")
      if(replicatesNode.isArray) {
        val replicates = replicatesNode.getElements
        while (replicates.hasNext){
          val replicate = replicates.next()
          if(replicate.has("biological_replicate_number") && replicateIds.contains(replicate.get("biological_replicate_number").asText()))
            printTree(replicate,"replicates."+replicate.get("biological_replicate_number").asText(),writer,metaList)
        }
      }
    }
  }

  /**
    * gets the "hard coded" exclusion categories, meant to be used for the particular cases
    * Files and Replicates should be always be there, other exclusions are managed from xml file with regex.
    */
  val exclusionCategories: java.util.ArrayList[String] ={
    val list = new java.util.ArrayList[String]()
    list.add("files")
    list.add("replicates")
    list
  }
  /**
    * loads from the xml "encodeMetadataConfig" the regex set to be excluded fromt he metadata.
    */
  var exclusionRegex: java.util.ArrayList[String] = new util.ArrayList[String]()


  /**
    * by giving an initial node, prints into the .meta file its metadata and its children's metadata also.
    * I use java arraylist as scala list cannot be put as var in the parameters.
    * @param node current node
    * @param parents path separated by dots for each level
    * @param writer file writer with the open .meta file
    * @param metaList list with already inserted meta to avoid duplication.
    */
  def printTree(node: JsonNode, parents: String, writer: PrintWriter, metaList: java.util.ArrayList[String]): Unit = {
    //base case, the node is value
    if(node.isValueNode && node.asText() != ""
    //&& !metaList.contains(node.asText())
    //uncomment this to avoid repeating metadata values.
    ) {
      writer.write(parents + "\t" + node.asText() + "\n")
      metaList.add(node.asText())
    }
    else {
      val fields: util.Iterator[String] = node.getFieldNames
      while (fields.hasNext) {
        val name = fields.next()
        if (!exclusionCategories.contains(name)) {
          val element = node.get(name)
          //base case when parents are empty
          val currentName = if (parents == "") name else parents + "." + name
          //check the regex
          var regexMatch = false
          for(i<-0 until exclusionRegex.size())
            if(currentName.matches(exclusionRegex.get(i)))
              regexMatch = true
          if(!regexMatch) {
            if (element.isArray) {
              val subElements = element.getElements
              while (subElements.hasNext)
                printTree(subElements.next(), currentName, writer, metaList)
            }
            else
              printTree(element, currentName, writer, metaList)
          }
        }
      }
    }
  }

  //--------------------------------------------SCHEMA SECTION-------------------------------------------
  /**
    * using information in the loader should arrange the files into a single folder
    * where data and metadata are paired as (file,file.meta) and should put also
    * the schema file inside the folder.
    * ENCODE schema file is not provided in the same folder as the data
    * for the moment the schemas have to be given locally.
    *
    * @param source contains specific download and sorting info.
    */
  def organize(source: GMQLSource): Unit = {
    source.datasets.foreach(dataset => {
      if(dataset.transformEnabled) {
        if (dataset.schemaLocation == SCHEMA_LOCATION.LOCAL) {
          val src = new File(dataset.schema)
          val dest = new File(source.outputFolder + "/" + dataset.outputFolder + File.separator +
            "Transformations" + File.separator + dataset.outputFolder + ".schema")

          try {
            Files.copy(src, dest)
            logger.info("Schema copied from: " + src.getAbsolutePath + " to " + dest.getAbsolutePath)
          }
          catch {
            case e: IOException => logger.error("could not copy the file " +
              src.getAbsolutePath + " to " + dest.getAbsolutePath)
          }
        }
      }
    })
  }
  //-------------------------------------UTILS AND DEPRECATED------------------------------------------------
  /**
    * extracts the gzipFile into the outputPath.
    *
    * @param gzipFile   full location of the gzip
    * @param outputPath full path of destination, filename included.
    */
  def unGzipIt(gzipFile: String, outputPath: String): Unit = {
    val bufferSize = 1024
    val buffer = new Array[Byte](bufferSize)

    try {
      val zis = new GZIPInputStream(new BufferedInputStream(new FileInputStream(gzipFile)))
      val newFile = new File(outputPath)
      val fos = new FileOutputStream(newFile)

      var ze: Int = zis.read(buffer)
      while (ze >= 0) {

        fos.write(buffer, 0, ze)
        ze = zis.read(buffer)
      }
      fos.close()
      zis.close()
    } catch {
      case e: IOException => logger.error("Couldnt UnGzip the file: " + outputPath, e)
    }
  }

/*
import java.util.Calendar
import scala.io.Source
  /**
    * deprecated
    * splits the metadata file into many .meta files as many rows the file has.
    *
    * @param source  contains specific download and sorting info.
    * @param dataset refers to the actual dataset being added
    */
  private def transformMetaFromDotMeta(source: GMQLSource, dataset: GMQLDataset): Unit = {
    logger.info("Splitting ENCODE metadata for dataset: " + dataset.outputFolder)

    val file: Elem = XML.loadFile(source.parameters.filter(_._1.equalsIgnoreCase("encode_metadata_configuration")).head._2)
    val metadataToInclude: Seq[String] = ((file\\"encode_metadata_config"\"parameter_list").filter(list =>
      (list\"@name").text.equalsIgnoreCase("encode_metadata_tsv"))\"parameter").filter(field =>
      (field\"@include").text.equalsIgnoreCase("true") && (field\"key").text.equalsIgnoreCase("field")).map(field =>{
      (field\\"value").text
    })
    val reportPath = source.outputFolder + File.separator + dataset.outputFolder +
      File.separator + "Downloads"+ File.separator + "report.tsv"
    val reportFile = Source.fromFile(reportPath).getLines()
    val reportFileWithoutHeader = reportFile.drop(1)
    val reportFileWithoutHeaderMapped: Iterator[(String, Array[String])] = reportFileWithoutHeader.map(line => (line.split("\t").head, line.split("\t")))
    val report: Seq[(String, Array[String])] = reportFileWithoutHeaderMapped.toSeq
    val reportHeader: Array[String] = Source.fromFile(reportPath).getLines().next().split("\t")

    val metadataPath = source.outputFolder + File.separator + dataset.outputFolder +
      File.separator + "Downloads"+ File.separator + "metadata.tsv"
    val transformationPath = source.outputFolder + File.separator + dataset.outputFolder +
      File.separator + "Transformations"

    val header = Source.fromFile(metadataPath).getLines().next().split("\t")
    val experimentAccession = header.lastIndexOf("Experiment accession")

    //this "File download URL" maybe should be in the parameters of the XML.
    val url = header.lastIndexOf("File download URL")
    //I have to implement log also here.
    val log = new FileLogger(transformationPath)
    Source.fromFile(metadataPath).getLines().drop(1).foreach(f = line => {
      //create file .meta
      val fields: Array[String] = line.split("\t")
      val fileName = fields(url).split(File.separator).last
      val metadataName = fileName.substring(0, fileName.lastIndexOf(".")) + ".meta" //this is the meta name

      //here have to change the log to receive Seq[String] as origin
      log.checkIfUpdate(
        metadataName,
        metadataPath,
        new File(metadataPath).getTotalSpace.toString,
        Calendar.getInstance.getTime.toString)

      val file = new File(transformationPath + File.separator + metadataName)
      val writer = new PrintWriter(file)

      for (i <- fields.indices) {
        if (fields(i).nonEmpty && metadataToInclude.contains(header(i)))
          writer.write(header(i).replace(" ","_") + "\t" + fields(i) + "\n")
      }
      val aux1 = fields(experimentAccession)
      val aux2 = report.filter(_._1.contains(aux1))
      val reportFields: Array[String] = aux2.head._2

      for(i <- reportFields.indices){
        if(reportFields(i).nonEmpty)
          writer.write(reportHeader(i).replace(" ","_") + "\t" + reportFields(i)+"\n")
      }
      writer.close()
      log.markAsUpdated(metadataName)

      logger.debug("File Created: " + source.outputFolder + File.separator + dataset.outputFolder + File.separator +
        "Transformations" + File.separator + metadataName)
    })
    log.saveTable()
  }*/
}
