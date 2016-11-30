package it.polimi.genomics.importer.DefaultImporter
import java.io.File
import java.net.URL

import it.polimi.genomics.importer.FileDatabase.FileLogger
import it.polimi.genomics.importer.GMQLImporter.{GMQLDownloader, GMQLSource}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.slf4j.LoggerFactory

import scala.sys.process._

/**
  * Created by NachoBook on 19/09/2016.
  * Handles HTTP connection for downloading files
  */
class HTTPDownloader extends GMQLDownloader {
  val logger = LoggerFactory.getLogger(this.getClass)
  /**
    * checks if the given URL exists
    *
    * @param path URL to check
    * @return URL exists
    */
  def urlExists(path: String): Boolean = {
    try {
      scala.io.Source.fromURL(path)
      true
    } catch {
      case _: Throwable => false
    }
  }
  /**
    * given a url and destination path, downloads that file into the path
    *
    * @param url  source file url.
    * @param path destination file path and name.
    */
  def downloadFileFromURL(url: String, path: String): Unit = {
    try {
      new URL(url) #> new File(path) !!;
      logger.info("Downloading: " + path + " from: " + url + " DONE")
    }
    catch{
      case e: Throwable => logger.error("Downloading: " + path + " from: " + url + " failed: ")
    }
  }
  /**
    * downloads the files from the source defined in the information
    * into the folder defined in the source and its dataset
    *
    * @param source configuration for the downloader, folders for input and output by regex and also for files.
    */
  override def download(source: GMQLSource): Unit = {
    if (urlExists(source.url)) {
      //same as FTP the mark to compare is done here because the iteration on http is based on http folders and not
      //on the source datasets.
      source.datasets.foreach(dataset => {
        if(dataset.downloadEnabled) {
          val outputPath = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads"
          if (!new File(outputPath).exists())
            new File(outputPath).mkdirs()
          val log = new FileLogger(outputPath)
          log.markToCompare()
          log.saveTable()
        }
      })

      recursiveDownload(source.url,source)

      source.datasets.foreach(dataset => {
        if(dataset.downloadEnabled) {
          val outputPath = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads"
          val log = new FileLogger(outputPath)
          log.markAsOutdated()
          log.saveTable()
        }
      })
    }
  }
  /**
    * recursively checks all folders and subfolders matching with the regular expressions defined in the source
    *
    * @param path       current path of the http connection
    * @param source     configuration for the downloader, folders for input and output by regex and also for files.
    */
  private def recursiveDownload(path: String, source: GMQLSource): Unit = {

    if (urlExists(path)) {
      val urlSource = scala.io.Source.fromURL(path)
      val result = urlSource.mkString
      val document: Document = Jsoup.parse(result)

      checkFolderForDownloads(path, document, source)
      downloadSubFolders(path, document, source)
    }
  }

  /**
    * given a folder, searches all the possible links to download and downloads if signaled by Updater and loader
    *
    * @param path     current directory
    * @param document current location  Jsoup document
    * @param source   contains download information
    */
  def checkFolderForDownloads(path: String, document: Document, source: GMQLSource): Unit = {
    for (dataset <- source.datasets) {
      if(dataset.downloadEnabled) {
        //If the container is table, I got the rows, if not I look for anchor tags to navigate the site
        val elements = if (source.parameters.filter(_._1.toLowerCase == "table").head._2.equalsIgnoreCase("true"))
          document.select("tr")
        else
          document.select("a")

        if (path.matches(dataset.parameters.filter(_._1.toLowerCase == "folder_regex").head._2)) {
          logger.info("Searching into: " + path)
          val outputPath = source.outputFolder + File.separator + dataset.outputFolder + File.separator + "Downloads"
          val log = new FileLogger(outputPath)

          if (!new java.io.File(outputPath).exists) {
            new java.io.File(outputPath).mkdirs()
          }

          for (i <- 0 until elements.size()) {
            //The url if is in a table, I have to get it from the row, if not, it has the href from anchor tag
            var url: String =
            if (source.parameters.filter(_._1.toLowerCase == "table").head._2.toLowerCase == "true")
              try {
                elements.get(i).text.trim.split(" ").filterNot(_.isEmpty)(
                  source.parameters.filter(_._1.toLowerCase == "name_index").head._2.toInt)
              } catch {
                case _: Throwable => ""
              }
            else
              elements.get(i).attr("href")

            if (url.startsWith("/"))
              url = url.substring(1)
            //if the file matches with a regex to download
            if (url.matches(dataset.parameters.filter(_._1.toLowerCase == "files_regex").head._2)) {
              //from the anchor tag, i need to get nextSibling's text, from table I join all the tds on the row
              val dateAndSize =
              if (source.parameters.filter(_._1.toLowerCase == "table").head._2.toLowerCase == "true")
                elements.get(i).text.trim.split(" ").filterNot(_.isEmpty)
              else
                elements.get(i).nextSibling().toString.trim.split(" ").filterNot(_.isEmpty)

              val date = dateAndSize(source.parameters.filter(_._1.toLowerCase == "date_index").head._2.toInt) + File.separator +
                dateAndSize(source.parameters.filter(_._1.toLowerCase == "hour_index").head._2.toInt)
              val size = dateAndSize(source.parameters.filter(_._1.toLowerCase == "size_index").head._2.toInt)

              val checkDownload = log.checkIfUpdate(url, path + File.separator + url, size, date)
              if (checkDownload) {
                downloadFileFromURL(path + url,outputPath + File.separator + url)
                log.markAsUpdated(url)
              }
            }
          }
          log.saveTable()
        }
      }
    }
  }

  /**
    * Finds all subfolders in the working directory and performs checkFolderForDownloads on it
    *
    * @param path     working directory
    * @param document current location Jsoup document
    * @param source   contains download information
    */
  def downloadSubFolders(path: String, document: Document, source: GMQLSource): Unit = {
    //directories is to avoid taking backward folders
    val folders = path.split(File.separator)
    var directories = List[String]()
    for (i <- folders) {
      if (directories.nonEmpty)
        directories = directories :+ directories.last + File.separator + i
      else
        directories = directories :+ i
    }

    val elements =
      if(source.parameters.filter(_._1.toLowerCase == "table").head._2.toLowerCase=="true")
        document.select("tr")
      else
        document.select("a")

    for (i <- 0 until elements.size()) {

      var url =
        if (source.parameters.filter(_._1.toLowerCase == "table").head._2.toLowerCase=="true")
          try {
            elements.get(i).text.trim.split(" ").filterNot(_.isEmpty)(source.parameters.filter(_._1.toLowerCase == "name_index").head._2.toInt)
          } catch {
            case _: Throwable => ""
          }
        else
          elements.get(i).attr("href")

      if (url.endsWith(File.separator) && !url.contains(".."+File.separator) && !directories.contains(url)) {
        if (url.startsWith(File.separator))
          url = url.substring(1)
        recursiveDownload(path + url, source)
      }
    }
  }
}
