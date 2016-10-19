package it.polimi.genomics.importer.GMQLImporter.Defaults

import java.io.File

import it.polimi.genomics.importer.FileLogger.FileLogger
import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION
import it.polimi.genomics.importer.GMQLImporter.{GMQLDataset, GMQLSource, GMQLTransformer}

/**
  * Created by Nacho on 10/13/16.
  */
object NULLTransformer extends GMQLTransformer {
  /**
    * using the information in the information should convert the downloaded files
    * into data and metadata as specified in GDM
    *
    * @param source contains specific download and sorting info.
    */
  override def transform(source: GMQLSource): Unit = {
    println("Starting transform for: " + source.outputFolder)
    source.datasets.foreach(dataset => {
      println("Transform for dataset: " + dataset.outputFolder)
      val folder = new File(source.outputFolder + "/" + dataset.outputFolder + "/Transformations")
      if (!folder.exists()) {
        folder.mkdirs()
      }
      transformData(source, dataset)
    })
    organize(source)
  }


  /**
    * Checks for which data has to be updated or added to the GMQL repository, and unGzips the needed files.
    * also saves in the Transform log, so when the Loader reads it, knows if the data should be updated, deleted or added.
    *
    * @param source contains specific download and sorting info.
    * @param dataset     refers to the actual dataset being added
    */
  private def transformData(source: GMQLSource, dataset: GMQLDataset): Unit = {
    val logDownload = new FileLogger(source.outputFolder + "/" + dataset.outputFolder + "/Downloads")
    val logTransform = new FileLogger(source.outputFolder + "/" + dataset.outputFolder + "/Transformations")
    logTransform.markAsOutdated()
    logDownload.filesToUpdate().foreach(file => {
      if (logTransform.checkIfUpdate(file.name, file.name, file.originSize, file.lastUpdate)) {
        print("copying file: " + file.name)
        copyFile(source.outputFolder + "/" + dataset.outputFolder + "/Downloads/" + file.name, source.outputFolder + "/" + dataset.outputFolder + "/Transformations/" + file.name)
        logTransform.markAsUpdated(file.name)
        println(" DONE")
      }
    })
    logDownload.markAsProcessed()
    logDownload.saveTable()
    logTransform.saveTable()
  }

  /**
    * for TCGA2BED all the files are downloaded organized, just need to
    * find the correct schema file and put it into the folder.
    * @param source contains all required information for organizing the data and metadata
    */
  override def organize(source: GMQLSource): Unit = {
    source.datasets.foreach(dataset=>{
      if(dataset.schemaLocation == SCHEMA_LOCATION.LOCAL){
        import java.io.{File, FileInputStream, FileOutputStream}
        val src = new File(dataset.schema)
        val dest = new File(source.outputFolder+"/"+dataset.outputFolder+"/Transformations/"+dataset.outputFolder+".schema")
        new FileOutputStream(dest) getChannel() transferFrom(
          new FileInputStream(src) getChannel, 0, Long.MaxValue )
        println("Schema copied from: " + src+" to "+dest)
      }
    })
  }

  /**
    * copies a file from origin to destination
    * @param origin path for origin file
    * @param destination path for destination file
    */
  private def copyFile(origin: String, destination: String): Unit ={
    import java.io.{File, FileInputStream, FileOutputStream}
    val src = new File(origin)
    val dest = new File(destination)
    new FileOutputStream(dest) getChannel() transferFrom(
      new FileInputStream(src) getChannel, 0, Long.MaxValue )
  }

}
