package it.polimi.genomics.importer.ENCODEImporter

import java.io._
import java.util.zip.GZIPInputStream

import com.google.common.io.Files
import it.polimi.genomics.importer.FileLogger.FileLogger
import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION
import it.polimi.genomics.importer.GMQLImporter.{GMQLDataset, GMQLSource, GMQLTransformer}
import org.slf4j.LoggerFactory

import scala.io.Source

/**
  * Created by Nacho on 10/13/16.
  * Object meant to be used for transform the data from ENCODE to data for GMQL,
  * files must be in the following format:
  *   - metadata file downloaded from ENCODE (1 single file for all the samples)
  *   - .gz data files downloaded from ENCODE.
  */
object ENCODETransformer extends GMQLTransformer {
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
      logger.info("Transformation for dataset: " + dataset.outputFolder)
      val folder = new File(source.outputFolder + "/" + dataset.outputFolder + "/Transformations")
      if (!folder.exists()) {
        logger.debug("Folder created: "+folder)
        folder.mkdirs()
      }
      transformData(source, dataset)
      transformMeta(source, dataset)
    })
    organize(source)
  }

  /**
    * Checks for which data has to be updated or added to the GMQL repository, and unGzips the needed files.
    * also saves in the Transform log, so when the Loader reads it,
    * knows if the data should be updated, deleted or added.
    *
    * @param source contains specific download and sorting info.
    * @param dataset     refers to the actual dataset being added
    */
  private def transformData(source: GMQLSource, dataset: GMQLDataset): Unit = {
    val logDownload = new FileLogger(source.outputFolder + "/" + dataset.outputFolder + "/Downloads")
    val logTransform = new FileLogger(source.outputFolder + "/" + dataset.outputFolder + "/Transformations")
    logTransform.markAsOutdated()
    logDownload.filesToUpdate().foreach(file =>{
      //this is to take out the ".gz"
      val name = file.name.substring(0, file.name.lastIndexOf("."))
      //should get file size, for the moment I pass the origin size just to have a value.
      if (logTransform.checkIfUpdate(name, file.name, file.originSize, file.lastUpdate)) {
        logger.debug("Start unGzipping: " + file.name)
        unGzipIt(
          source.outputFolder + "/" + dataset.outputFolder + "/Downloads/" + file.name,
          source.outputFolder + "/" + dataset.outputFolder + "/Transformations/" + name)
        logTransform.markAsUpdated(name)
        logger.info("UnGzipping: " + file.name+" DONE")
      }
    })
    logDownload.markAsProcessed()
    logDownload.saveTable()
    logTransform.saveTable()
  }

  /**
    * splits the metadata file into many .meta files as many rows the file has.
    *
    * @param source contains specific download and sorting info.
    * @param dataset     refers to the actual dataset being added
    */
  private def transformMeta(source: GMQLSource, dataset: GMQLDataset): Unit = {
    logger.info("Splitting ENCODE metadata for dataset: " + dataset.outputFolder)
    val header = Source.fromFile(
      source.outputFolder + "/" +
        dataset.outputFolder + "/Downloads/" +
        dataset.outputFolder + ".tsv").getLines().next().split("\t")

    //this "File download URL" maybe should be in the parameters of the XML.
    val url = header.lastIndexOf("File download URL")
    Source.fromFile(
      source.outputFolder + "/" + dataset.outputFolder +
        "/Downloads/" + dataset.outputFolder + ".tsv").getLines().drop(1).foreach(f = line => {
      //create file .meta
      val fields = line.split("\t")
      val aux1 = fields(url).split("/").last
      val aux2 = aux1.substring(0, aux1.lastIndexOf(".")) + ".meta" //this is the meta name
      val file = new File(source.outputFolder + "/" + dataset.outputFolder + "/Transformations/" + aux2)

      val writer = new PrintWriter(file)
      for (i <- 0 until fields.size) {
        if (fields(i).nonEmpty)
          writer.write(header(i) + "\t" + fields(i) + "\n")
      }
      writer.close()
      logger.debug("File Created: " + source.outputFolder + "/" + dataset.outputFolder + "/Transformations/" + aux2)
    })
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

      //Gzip files are meant to be single files.
      val zis = new GZIPInputStream(new BufferedInputStream(new FileInputStream(gzipFile))) //gzip
      val newFile = new File(outputPath)
      val fos = new FileOutputStream(newFile)

      var ze: Int = zis.read(buffer) //gzip
      while (ze >= 0) {
        //gzip
        fos.write(buffer, 0, ze)
        ze = zis.read(buffer) //gzip
      }
      fos.close()
      zis.close()
    } catch {
      case e: IOException => logger.error("Couldnt UnGzip the file: " + outputPath,e)
    }
  }

  /**
    * using information in the loader should arrange the files into a single folder
    * where data and metadata are paired as (file,file.meta) and should put also
    * the schema file inside the folder.
    * ENCODE schema file is not provided in the same folder as the data
    * for the moment the schemas have to be given locally.
    *
    *  @param source contains specific download and sorting info.
    */
  override def organize(source: GMQLSource): Unit = {
    source.datasets.foreach(dataset => {
      if (dataset.schemaLocation == SCHEMA_LOCATION.LOCAL) {
        val src = new File(dataset.schema)
        val dest = new File(source.outputFolder + "/" + dataset.outputFolder +
          "/Transformations/" + dataset.outputFolder + ".schema")

        try {
          Files.copy(src,dest)
          logger.info("Schema copied from: " + src.getAbsolutePath + " to " + dest.getAbsolutePath)
        }
        catch {
          case e: IOException => logger.error("could not copy the file "+
            src.getAbsolutePath + " to " + dest.getAbsolutePath)
        }
      }
    })
  }
}
