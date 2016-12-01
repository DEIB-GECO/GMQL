package it.polimi.genomics.importer.DefaultImporter

import java.io.File

import it.polimi.genomics.importer.FileDatabase.{FileDatabase,STAGE}
import it.polimi.genomics.importer.DefaultImporter.utils.FTP
import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION
import it.polimi.genomics.importer.GMQLImporter.{GMQLDataset, GMQLDownloader, GMQLSource}
import org.slf4j.LoggerFactory

/**
  * Created by Nacho on 10/13/16.
  */
class FTPDownloader extends GMQLDownloader {
  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * downloads the files from the source defined in the information
    * into the folder defined in the source
    *
    * @param source contains specific download and sorting info.
    */
  override def download(source: GMQLSource): Unit = {
    if(source.downloadEnabled) {
      logger.info("Starting download for: " + source.name)
      if (!new java.io.File(source.outputFolder).exists) {
        new java.io.File(source.outputFolder).mkdirs()
      }
      val ftp = new FTP()

      //the mark to compare is done here because the iteration on ftp is based on ftp folders and not
      //on the source datasets.
      val sourceId = FileDatabase.sourceId(source.name)
      source.datasets.foreach(dataset => {
        if (dataset.downloadEnabled) {
          val datasetId = FileDatabase.datasetId(sourceId,dataset.name)
          FileDatabase.markToCompare(datasetId,STAGE.DOWNLOAD)
        }
      })

      logger.debug("trying to connect to FTP: " + source.url +
        " - username: " + source.parameters.filter(_._1 == "username").head._2 +
        " - password: " + source.parameters.filter(_._1 == "password").head._2)
      if (ftp.connectWithAuth(
        source.url,
        source.parameters.filter(_._1 == "username").head._2,
        source.parameters.filter(_._1 == "password").head._2).getOrElse(false)) {

        logger.info("Connected to ftp: " + source.url)
        val workingDirectory = ftp.workingDirectory()
        ftp.disconnect()
        recursiveDownload(workingDirectory, source)

        source.datasets.foreach(dataset => {
          if (dataset.downloadEnabled) {
            val datasetId = FileDatabase.datasetId(sourceId,dataset.name)
            FileDatabase.markToCompare(datasetId,STAGE.DOWNLOAD)
          }
        })
      }
      else
        logger.warn("ftp connection with " + source.url + " couldn't be handled.")
    }
  }

  /**
    * recursively checks all folders and subfolders matching with the regular expressions defined in the information
    *
    * @param workingDirectory    current folder of the ftp connection
    * @param source configuration for the downloader, folders for input and output by regex and also for files.
    */
  private def recursiveDownload(workingDirectory: String, source: GMQLSource): Unit = {
    checkFolderForDownloads(workingDirectory, source)
    downloadSubfolders(workingDirectory, source)
  }

  /**
    * given a folder, searches all the possible links to download and downloads if signaled by Updater and information
    * puts all content into information.outputFolder/dataset.outputFolder/Downloads/
    * (this is because TCGA2BED does no transform and we dont want just to copy the files).
    *
    * @param workingDirectory    current state of the ftp connection
    * @param source configuration for downloader, folders for input and output by regex and also for files
    */
  private def checkFolderForDownloads(workingDirectory: String, source: GMQLSource): Unit = {
    val ftp = new FTP()
    if (ftp.connectWithAuth(
      source.url,
      source.parameters.filter(_._1 == "username").head._2,
      source.parameters.filter(_._1 == "password").head._2).getOrElse(false)) {
      val sourceId = FileDatabase.sourceId(source.name)
      ftp.cd(workingDirectory)
      for (dataset <- source.datasets) {
        if (dataset.downloadEnabled) {
          val datasetId = FileDatabase.datasetId(sourceId,dataset.name)
          if (ftp.workingDirectory().matches(dataset.parameters.filter(_._1 == "folder_regex").head._2)) {
            val outputPath = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads"

            logger.info("Searching: " + ftp.workingDirectory())
            if (!new java.io.File(outputPath).exists) {
              new java.io.File(outputPath).mkdirs()
            }

            val files = ftp.listFiles().filter(_.isFile).filter(_.getName.matches(
              dataset.parameters.filter(_._1 == "files_regex").head._2))

            for (file <- files) {
              val url = ftp.workingDirectory() + File.separator + file.getName
              val fileId = FileDatabase.fileId(datasetId,url,STAGE.DOWNLOAD,file.getName)
              if(FileDatabase.checkIfUpdateFile(
                fileId,
                "hash not here yet",
                file.getSize.toString,
                file.getTimestamp.getTime.toString)){
                val nameAndCopyNumber: (String, Int) = FileDatabase.getFileNameAndCopyNumber(fileId)
                val name =
                  if(nameAndCopyNumber._2==1)nameAndCopyNumber._1
                  else nameAndCopyNumber._1.replaceFirst("\\.","_"+nameAndCopyNumber._2+".")
                val outputUrl = outputPath + File.separator + name

                logger.debug("Starting download of: " + url)
                var downloaded = ftp.downloadFile(file.getName, outputUrl)
                var timesTried = 0
                while (!downloaded && timesTried < 4) {
                  downloaded = ftp.downloadFile(file.getName, outputUrl)
                  timesTried += 1
                }
                if (!downloaded) {
                  logger.error("Downloading: " + url + " FAILED")
                  FileDatabase.markAsFailed(fileId)
                }
                else {
                  logger.info("Downloading: " + url + " DONE")
                  FileDatabase.markAsUpdated(fileId,new File(outputUrl).getTotalSpace.toString)
                }
              }
            }
          }
        }
      }
    }
    else
      logger.error("connection lost with FTP, skipping "+workingDirectory)
  }

  /**
    * Finds all subfolders in the working directory and performs checkFolderForDownloads on it
    *
    * @param workingDirectory    current folder of the ftp connection
    * @param source configuration for downloader, folders for input and output by regex and also for files
    */
  private def downloadSubfolders(workingDirectory: String, source: GMQLSource): Unit = {

    val ftp = new FTP()
    if (ftp.connectWithAuth(
      source.url,
      source.parameters.filter(_._1 == "username").head._2,
      source.parameters.filter(_._1 == "password").head._2).getOrElse(false)) {
      ftp.cd(workingDirectory)
      logger.info("working directory: " + workingDirectory)
      val directories = ftp.listDirectories()
      ftp.disconnect()
      directories.foreach(directory =>
        recursiveDownload(
          if (workingDirectory.endsWith(File.separator)) workingDirectory + directory.getName
          else workingDirectory + File.separator + directory.getName,
          source))
    }
    else
      logger.error("connection lost with FTP, skipped " + workingDirectory)
  }

  /**
    * deprecated
    *
    * this method is meant to be used for downloading ftp schema
    * puts the schema in a folder called "downloadedSchema" with chosen name
    *
    * maybe I have to change the information parameter.
    * and maybe the place to do this is not here.
    *
    * @param source  configuration for downloader, folders for input and output by regex and also for files
    * @param dataset contains information for parameters of the url
    */
  def downloadSchema(source: GMQLSource, dataset: GMQLDataset): Unit = {
    if (!new java.io.File(source.outputFolder).exists) {
      new java.io.File(source.outputFolder).mkdirs()
    }
    val ftp = new FTP()
    if (dataset.schemaLocation == SCHEMA_LOCATION.REMOTE &&
      ftp.connectWithAuth(source.url,
        source.parameters.filter(_._1 == "username").head._2,
        source.parameters.filter(_._1 == "password").head._2).getOrElse(false)) {
      if (!ftp.downloadFile(dataset.schemaUrl, source.outputFolder +
        File.separator + dataset.outputFolder + File.separator + dataset.outputFolder + ".schema"))
        logger.error("File: " + dataset.schemaUrl + " Couldn't be downloaded")
      ftp.disconnect()
    }
    else
      logger.error("ftp connection with " + source.url + " couldn't be handled." +
        "username:" + source.parameters.filter(_._1 == "username").head._2 +
        "password:" + source.parameters.filter(_._1 == "password").head._2)
  }
}
