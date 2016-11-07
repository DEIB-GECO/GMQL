package it.polimi.genomics.importer.DefaultImporter

import java.io.{File, IOException}

import com.google.common.io.Files
import it.polimi.genomics.importer.FileLogger.FileLogger
import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION
import it.polimi.genomics.importer.GMQLImporter.{GMQLDataset, GMQLSource, GMQLTransformer}
import org.slf4j.LoggerFactory

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
    val logDownload = new FileLogger(
      source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads")
    val logTransform = new FileLogger(
      source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Transformations")
    logTransform.markAsOutdated()
    logDownload.files.foreach(file => {
      if (logTransform.checkIfUpdate(file.name, file.name, file.originSize, file.lastUpdate)) {
        try {
          Files.copy(new File(source.outputFolder + File.separator + dataset.outputFolder +
            File.separator + "Downloads" + File.separator + file.name),
            new File(source.outputFolder + File.separator + dataset.outputFolder +
              File.separator + "Transformations" + File.separator + file.name))
          logTransform.markAsUpdated(file.name)
          logger.info("File: " + file.name + " copied into " + source.outputFolder + File.separator +
            dataset.outputFolder + File.separator + "Transformations" + File.separator + file.name)
        }
        catch {
          case e: IOException => logger.error("could not copy the file " +
            source.outputFolder + File.separator + dataset.outputFolder +
            File.separator + "Downloads" + File.separator + file.name + " to " +
            source.outputFolder + File.separator + dataset.outputFolder +
            File.separator + "Transformations" + File.separator + file.name, e)
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
          import java.io.{File, FileInputStream, FileOutputStream}
          val src = new File(dataset.schema)
          val dest = new File(source.outputFolder + File.separator + dataset.outputFolder + File.separator +
            "Transformations" + File.separator + dataset.outputFolder + ".schema")
          new FileOutputStream(dest) getChannel() transferFrom(
            new FileInputStream(src) getChannel, 0, Long.MaxValue)
          logger.info("Schema copied into " + dest)
        }
      }
    })
  }
}
