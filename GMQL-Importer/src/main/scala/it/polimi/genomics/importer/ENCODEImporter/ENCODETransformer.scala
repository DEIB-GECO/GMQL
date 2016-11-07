package it.polimi.genomics.importer.ENCODEImporter

import java.io.{File, _}
import java.util.Calendar
import java.util.zip.GZIPInputStream

import com.google.common.io.Files
import it.polimi.genomics.importer.FileLogger.FileLogger
import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION
import it.polimi.genomics.importer.GMQLImporter.{GMQLDataset, GMQLSource, GMQLTransformer}
import org.slf4j.LoggerFactory

import scala.Seq
import scala.io.Source
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

  /**
    * ENCODE data comes in .gz containers and metadata comes all together in a single file
    * splits the metadata file into many .meta files, and extracts the .gz containers.
    *
    * @param source contains specific download and sorting info.
    */
  override def transform(source: GMQLSource): Unit = {
    logger.info("Starting transformation for: " + source.outputFolder)
    source.datasets.foreach(dataset => {
      val datasetOutputFolder = source.outputFolder + File.separator + dataset.outputFolder + File.separator
      val report = new File (datasetOutputFolder + "Downloads"+ File.separator + "report.tsv")
      val metadata = new File(datasetOutputFolder + "Downloads"+ File.separator + "metadata.tsv")
      if(dataset.transformEnabled) {
        if(report.exists() && metadata.exists()) {
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
      }
    })
    organize(source)
  }

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
    logTransform.markAsOutdated()
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
    logDownload.markAsProcessed()
    logDownload.saveTable()
    logTransform.saveTable()
  }

  /**
    * splits the metadata file into many .meta files as many rows the file has.
    *
    * @param source  contains specific download and sorting info.
    * @param dataset refers to the actual dataset being added
    */
  private def transformMeta(source: GMQLSource, dataset: GMQLDataset): Unit = {
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
  }

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
      val zis = new GZIPInputStream(new BufferedInputStream(new FileInputStream(gzipFile))) //gzip
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
}
