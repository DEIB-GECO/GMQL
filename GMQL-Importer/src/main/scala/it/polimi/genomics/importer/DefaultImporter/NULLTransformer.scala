package it.polimi.genomics.importer.DefaultImporter

import java.io.{File, IOException, PrintWriter}

import com.google.common.io.Files
import it.polimi.genomics.importer.FileLogger.FileLogger
import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION
import it.polimi.genomics.importer.GMQLImporter.{GMQLDataset, GMQLSource, GMQLTransformer}
import org.slf4j.LoggerFactory

import scala.io.Source

/**
  * Created by Nacho on 10/13/16.
  */
class NULLTransformer extends GMQLTransformer {
  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * using the information in the information should convert the downloaded files
    * into data and metadata as specified in GDM
    *
    * @param source contains specific download and sorting info.
    */
  override def transform(source: GMQLSource): Unit = {
    logger.info("Starting transformation for: " + source.outputFolder)
    source.datasets.foreach(dataset => {
      if(dataset.transformEnabled) {
        logger.info("Transformation for dataset: " + dataset.outputFolder)
        val folder = new File(source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Transformations")
        if (!folder.exists()) {
          folder.mkdirs()
        }
        transformData(source, dataset)
      }
    })
    organize(source)
  }


  /**
    * Checks for which data has to be updated or added to the GMQL repository, and unGzips the needed files.
    * also saves in the Transform log, so when the Loader reads it, knows if the data should be updated, deleted or added.
    *
    * @param source  contains specific download and sorting info.
    * @param dataset refers to the actual dataset being added
    */
  private def transformData(source: GMQLSource, dataset: GMQLDataset): Unit = {
    val downloadPath = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads"
    val transformPath = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Transformations"

    val logDownload = new FileLogger(downloadPath)
    val logTransform = new FileLogger(transformPath)

    logTransform.markAsOutdated()
    logDownload.files.foreach(file => {
      if (logTransform.checkIfUpdate(file.name, file.name, file.originSize, file.lastUpdate)) {
        try {
          Files.copy(new File(downloadPath + File.separator + file.name),
            new File(transformPath + File.separator + file.name))
          logTransform.markAsUpdated(file.name)
          logger.info("File: " + file.name + " copied into " + transformPath + File.separator + file.name)
        }
        catch {
          case e: IOException => logger.error("could not copy the file " +
            downloadPath + File.separator + file.name + " to " +
            transformPath + File.separator + file.name, e)
        }
      }
    })
    logDownload.markAsProcessed()
    logDownload.saveTable()
    logTransform.saveTable()
  }

  /**
    * for TCGA2BED all the files are downloaded organized, just need to
    * find the correct schema file and put it into the folder.
    *
    * @param source contains all required information for organizing the data and metadata
    */
  def organize(source: GMQLSource): Unit = {
    source.datasets.foreach(dataset => {
      if(dataset.transformEnabled) {
        if (dataset.schemaLocation == SCHEMA_LOCATION.LOCAL) {
          val src = new File(dataset.schema)
          val dest = new File(source.outputFolder + File.separator + dataset.outputFolder + File.separator +
            "Transformations" + File.separator + dataset.outputFolder + ".schema")
          try {
            Files.copy(src, dest)
            logger.info("Schema copied into " + dest)
          }
          catch{
            case e: IOException => logger.error("Schema couldn't be copied",e)
          }
        }
      }
    })
  }

  /**
    * changes the name of key attribute in metadata.
    * replaces all parts of the key value that matches a regular expression.
    * @param changeKeys pair regex, replacement. uses regex.replaceAll(_1,_2)
    * @param metadataFilePath origin file of metadata.
    */
  def changeMetadataKeys(changeKeys: Seq[(String,String)], metadataFilePath: String): Unit ={
    if(new File(metadataFilePath).exists()){
      val tempFile = metadataFilePath+".temp"
      val writer = new PrintWriter(tempFile)

      Source.fromFile(metadataFilePath).getLines().foreach(line=>{
        if(line.split("\t").length==2) {
          var metadataKey = line.split("\t")(0)
          val metadataValue = line.split("\t")(1)
          changeKeys.foreach(change => {
            metadataKey = change._1.r.replaceAllIn(metadataKey, change._2).replace(" ","_")
          })
          writer.write(metadataKey + "\t" + metadataValue + "\n")
        }
      })

      writer.close()

      try {
        Files.copy(new File(tempFile), new File(metadataFilePath))
        new File(tempFile).delete()
      }
      catch {
        case e: IOException => logger.error("could not change metadata key on the file " + metadataFilePath)
      }
    }
  }
}
