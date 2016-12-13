package it.polimi.genomics.importer.GMQLImporter

import java.io.{File, FileWriter, IOException, PrintWriter}

import com.google.common.io.Files
import it.polimi.genomics.importer.DefaultImporter.schemaFinder
import it.polimi.genomics.importer.FileDatabase.{FileDatabase, STAGE}
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.xml.XML

/**
  * Created by nachon on 12/6/16.
  */
object Integrator {
  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Transforms the data and metadata into GDM friendly formats using the transformers.
    * Normalizes the metadata key names and values.
    * Normalizes the region data values.
    * Puts the schema file into the transformations folders.
    * @param source source to be integrated.
    */
  def integrate(source: GMQLSource): Unit = {
    if (source.transformEnabled) {
      logger.info("Starting integration for: " + source.outputFolder)
      val sourceId = FileDatabase.sourceId(source.name)
      //I load the renaming list from the source parameters.

      val metadataRenaming: Seq[(String, String)] =
        try {
          (XML.loadFile(source.rootOutputFolder + File.separator + source.parameters
            .filter(_._1.equalsIgnoreCase("metadata_replacement")).head._2) \\ "metadata_replace_list" \ "metadata_replace")
            .map(replacement => ((replacement \ "regex").text, (replacement \ "replace").text))
        }
        catch {
          case e: IOException =>
            logger.error("not valid metadata replacement xml file: ")
            Seq[(String, String)]()
          case e: NoSuchElementException =>
            logger.info("no metadata replacement defined for: "+source.name)
            Seq[(String, String)]()
        }

      source.datasets.foreach(dataset => {
        if (dataset.transformEnabled) {
          val datasetId = FileDatabase.datasetId(sourceId, dataset.name)

          val datasetOutputFolder = source.outputFolder + File.separator + dataset.outputFolder
          val downloadsFolder = datasetOutputFolder + File.separator + "Downloads"
          val transformationsFolder = datasetOutputFolder + File.separator + "Transformations"

          // puts the schema into the transformations folder.
          if(schemaFinder.downloadSchema(source.rootOutputFolder,dataset,transformationsFolder))
            logger.debug("Schema downloaded for: "+dataset.name)
          else
            logger.warn("Schema not found for: "+dataset.name)

          val folder = new File(transformationsFolder)
          if (!folder.exists()) {
            folder.mkdirs()
            logger.debug("Folder created: " + folder)
          }
          logger.info("Transformation for dataset: " + dataset.name)

          FileDatabase.markToCompare(datasetId, STAGE.TRANSFORM)
          //id, filename, copy number.
          FileDatabase.getFilesToProcess(datasetId, STAGE.DOWNLOAD).foreach(file => {
            val originalFileName =
              if (file._3 == 1) file._2
              else file._2.replaceFirst("\\.", "_" + file._3 + ".")

            val fileDownloadPath = downloadsFolder + File.separator + originalFileName
            Class
              .forName(source.transformer)
              .newInstance.asInstanceOf[GMQLTransformer]
              .getCandidateNames(originalFileName)
              .map(candidateName => {
                FileDatabase.fileId(datasetId, fileDownloadPath, STAGE.TRANSFORM, candidateName)
              })
              .foreach(fileId => {
                val fileNameAndCopyNumber = FileDatabase.getFileNameAndCopyNumber(fileId)
                val name =
                  if (fileNameAndCopyNumber._2 == 1) fileNameAndCopyNumber._1
                  else fileNameAndCopyNumber._1.replaceFirst("\\.", "_" + fileNameAndCopyNumber._2 + ".")
                val originDetails = FileDatabase.getFileDetails(file._1)

                if (FileDatabase.checkIfUpdateFile(fileId, originDetails._1, originDetails._2, originDetails._3)) {
                  Class.forName(source.transformer).newInstance.asInstanceOf[GMQLTransformer]
                    .transform(source,downloadsFolder, transformationsFolder, originalFileName, name)
                  val fileTransformationPath = transformationsFolder + File.separator + name
                  //add copy numbers if needed.
                  if (name.endsWith(".meta")){
                    val maxCopy = FileDatabase.getMaxCopyNumber(datasetId, file._2, STAGE.DOWNLOAD)
                    if(maxCopy>1){
                      val writer = new FileWriter(fileTransformationPath, true)
                      writer.write("manually_curated|number_of_copies\t" + maxCopy + "\n")
                      writer.write("manually_curated|copy_number\t" + file._3 + "\n")
                      writer.write("manually_curated|file_name_replaced\ttrue\n")
                      writer.close()
                    }
                    //metadata renaming. (standardizing of the metadata values should happen here also.
                    changeMetadataKeys(metadataRenaming, fileTransformationPath)
                  }
                  //standardization of the region data should be here.
                  FileDatabase.markAsUpdated(fileId, new File(fileTransformationPath).getTotalSpace.toString)
                }
              })
          })
          FileDatabase.markAsOutdated(datasetId, STAGE.TRANSFORM)
          FileDatabase.markAsProcessed(datasetId, STAGE.DOWNLOAD)
        }
      })
    }
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
        else{
          logger.warn("file: "+metadataFilePath+" should have 2 columns. Check this line that was excluded: "+line)
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
