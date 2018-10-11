package it.polimi.genomics.repository.federated

import java.io.{FileInputStream, InputStream}

import it.polimi.genomics.repository.FSRepository.FS_Utilities
import it.polimi.genomics.repository.Utilities
import it.polimi.genomics.repository.federated.communication.{DownloadStatus, NotFound, NotFoundException}
import org.slf4j.{Logger, LoggerFactory}


class GF_Interface private {

  val logger: Logger = LoggerFactory.getLogger(Utilities.getClass)

  private val api = GF_Communication.instance()
  private val repo = Utilities().getRepository()

  def importDataset (jobId: String, dsName: String, location: String) = {
    val path = Utilities().getTempDir("federated")
    api.importDataset(jobId, dsName, location, path)
  }

  def checkImportStatus (jobId: String, dsName: String) : DownloadStatus = {
    api.getDownloadStatus(jobId, dsName)
  }

  // @throws NotFoundException
  def listPartialResultFiles(jobId: String, dsName: String) : List[String] = {

    val folder = Utilities().getResultDir("federated") + "/" + jobId + "/" + dsName + "/"

    if( FS_Utilities.checkExists(folder) ) {
      logger.info("Listing folder "+folder)
      FS_Utilities.listFiles(folder)
    } else {
      logger.error("Folder "+folder+" does not exists.")
      throw new NotFoundException()
    }

  }

  // @throws NotFoundException
  def fileStream(jobId: String, dsName: String, fileName: String) : InputStream = {

    val folder_path = Utilities().getResultDir("federated") + "/" + jobId + "/" + dsName
    val file_path = folder_path + "/" +fileName

    if( FS_Utilities.checkExists(folder_path) ) {
      logger.info("Streaming file "+file_path)
      FS_Utilities.getStream(file_path)
    } else {
      logger.error("Folder "+folder_path+" does not exists.")
      throw new NotFoundException()
    }



  }

  def deletePartialResult(jobId: String, dsName: String) = {

    logger.info("Deleting partial result: "+jobId+" "+dsName)

    val path = Utilities().getResultDir("federated") + "/" + jobId + "/" + dsName+ "/"
    FS_Utilities.deleteDFSDir(path)

  }

}

object GF_Interface {

  private var _instance : GF_Interface = null
  def instance() = {
    if (_instance == null)
      _instance = new GF_Interface()
    _instance
  }
}