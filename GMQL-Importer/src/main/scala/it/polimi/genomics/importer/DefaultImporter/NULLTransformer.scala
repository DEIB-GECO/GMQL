package it.polimi.genomics.importer.DefaultImporter

import java.io.{File, FileWriter, IOException, PrintWriter}

import com.google.common.io.Files
import it.polimi.genomics.importer.FileDatabase.{FileDatabase, STAGE}
import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION
import it.polimi.genomics.importer.GMQLImporter.{GMQLSource, GMQLTransformer}
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
    val sourceId = FileDatabase.sourceId(source.name)
    source.datasets.foreach(dataset => {
      if(dataset.transformEnabled) {
        logger.info("Transformation for dataset: " + dataset.outputFolder)
        val folder = new File(source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Transformations")
        if (!folder.exists()) {
          folder.mkdirs()
        }
        val datasetId = FileDatabase.datasetId(sourceId,dataset.name)
        FileDatabase.markToCompare(datasetId,STAGE.TRANSFORM)
        transformData(source, datasetId,dataset.outputFolder)
        FileDatabase.markAsProcessed(datasetId,STAGE.DOWNLOAD)
      }
    })
    organize(source)
  }


  /**
    * Checks for which data has to be updated or added to the GMQL repository, and unGzips the needed files.
    * also saves in the Transform log, so when the Loader reads it, knows if the data should be updated, deleted or added.
    *
    * @param source  contains specific download and sorting info.
    * @param datasetId refers to the actual dataset being added
    * @param datasetOutputFolder output folder for the dataset.
    */
  private def transformData(source: GMQLSource, datasetId: Int, datasetOutputFolder: String): Unit = {
    val downloadPath = source.outputFolder + File.separator + datasetOutputFolder + File.separator + "Downloads"
    val transformPath = source.outputFolder + File.separator + datasetOutputFolder + File.separator + "Transformations"

    FileDatabase.getFilesToProcess(datasetId, STAGE.DOWNLOAD).foreach(file => {
      //the name is going to be the same
      val candidateName =
      if (file._3 == 1) file._2
      else file._2.replaceFirst("\\.", "_" + file._3 + ".")

      val originUrl = downloadPath + File.separator + candidateName

      val fileId = FileDatabase.fileId(datasetId, originUrl, STAGE.TRANSFORM, candidateName)
      val fileNameAndCopyNumber = FileDatabase.getFileNameAndCopyNumber(fileId)
      val name =
        if (fileNameAndCopyNumber._2 == 1) fileNameAndCopyNumber._1
        else fileNameAndCopyNumber._1.replaceFirst("\\.", "_" + fileNameAndCopyNumber._2 + ".")

      val url = transformPath + File.separator + name

      val originDetails = FileDatabase.getFileDetails(file._1)

      val checkIfUpdate = FileDatabase.checkIfUpdateFile(fileId, originDetails._1, originDetails._2, originDetails._3)
      //      I commented this because for now the transformation has to be for every file.
      //      if  (checkIfUpdate) {
      try {
        Files.copy(new File(originUrl),
          new File(url))
        //here have to add the metadata of copy number and total copies
        if (url.endsWith(".meta")) {
          val numberOfCopies = FileDatabase.getMaxCopyNumber(datasetId, file._2, STAGE.DOWNLOAD)
          if (numberOfCopies > 1) {
            val writer = new FileWriter(url, true)
            writer.write("manually_curated|number_of_copies\t" + numberOfCopies + "\n")
            writer.write("manually_curated|copy_number\t" + file._3 + "\n")
            writer.write("manually_curated|file_name_replaced\ttrue\n")
            writer.close()
          }
        }
        FileDatabase.markAsUpdated(fileId, new File(url).getTotalSpace.toString)
        logger.info("File: " + file._2 + " copied into " + url)
      }
      catch {
        case e: IOException => logger.error("could not copy the file " +
          originUrl + " to " +
          url, e)
      }
      //      }
    })
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
          val src = new File(dataset.schemaUrl)
          val dest = new File(source.outputFolder + File.separator + dataset.outputFolder + File.separator +
            "Transformations" + File.separator + dataset.name + ".schema")
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
