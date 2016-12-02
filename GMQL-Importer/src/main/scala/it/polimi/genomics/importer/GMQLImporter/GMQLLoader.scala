package it.polimi.genomics.importer.GMQLImporter

import it.polimi.genomics.importer.FileDatabase.{FileDatabase,STAGE}
import it.polimi.genomics.repository.FSRepository.{DFSRepository, LFSRepository}
import it.polimi.genomics.repository.GMQLRepository.{GMQLDSNotFound, GMQLSample}
import org.slf4j.LoggerFactory
import java.io.File
/**
  * Created by Nacho on 10/17/16.
  */
class GMQLLoader {

  val logger = LoggerFactory.getLogger(this.getClass)
  /**
    * using information in the information, will insert into GMQLRepository the files
    * already downloaded, transformed and organized.
    *
    * Files have to be in the folder: information.outputFolder/dataset.outputFolder/Transformations/
    * and have to be sorted as pairs (file,file.meta) and also the .schema file have to be in the same folder.
    *
    * The process will look for every file ".meta" and will try to get its pair file.
    *
    * The GMQLTransformer should put inside the folder just the necessary files.
    *
    * @param source contains files location and datasets organization
    */
  def loadIntoGMQL(source: GMQLSource): Unit = {
    val repo = new DFSRepository
    val gmqlUser = source.parameters.filter(_._1=="gmql_user").head._2
    val stage = STAGE.TRANSFORM
    logger.debug("Preparing for loading datasets into GMQL")
    source.datasets.foreach(dataset =>{
      logger.trace("dataset "+dataset.name)
      if(dataset.loadEnabled) {
        val path = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Transformations"

        val listAdd = new java.util.ArrayList[GMQLSample]()

        val datasetId = FileDatabase.datasetId(FileDatabase.sourceId(source.name),dataset.name)

        FileDatabase.getFilesToProcess(datasetId,stage).filter(_._2.endsWith(".meta")).foreach(file => {
          val fileName = if(file._3 == 1) file._2 else file._2.replaceFirst("\\.","_"+file._3+".")
          try {
            listAdd.add(GMQLSample(
              path + File.separator + fileName.substring(0, fileName.lastIndexOf(".meta")),
              path + File.separator + fileName,
              null))
          }catch {
            case e: Throwable => logger.warn("data or metadata files missing: "+ path + File.separator + fileName + ". more details: "+e.getMessage)
          }
        })

        if (listAdd.size() > 0) {
          //CHANGE GMQLUSER TO SOURCE PARAMETERS
          logger.debug("Trying to add " + dataset.name +" to user: "+ gmqlUser)
          val datasetName = source.name + "_" + dataset.name
          //if repo exists I do DELETE THEN ADD
          //if(repo.DSExists())
          try{
            repo.DeleteDS(datasetName,gmqlUser)
          }catch {
            case e:GMQLDSNotFound => logger.debug("Dataset " + datasetName + " is not defined before!!")
          }

          try {
            repo.importDs(
              datasetName,
              gmqlUser,
              listAdd,
              path + File.separator + dataset.name + ".schema")
            logger.info("import for dataset "+dataset.name+" completed")
          }
          catch {
            case e: Throwable => logger.error("import failed: " + e.getMessage)
          }
          FileDatabase.markAsProcessed(datasetId,stage)
        }
        else
          logger.debug("dataset "+dataset.name+" has no files to be loaded")
      }
      else
        logger.trace("dataset "+dataset.name+" not included to load.")
    })
  }
}
