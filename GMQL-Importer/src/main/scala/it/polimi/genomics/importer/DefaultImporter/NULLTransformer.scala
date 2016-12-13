package it.polimi.genomics.importer.DefaultImporter

import java.io.{File, IOException}

import com.google.common.io.Files
import it.polimi.genomics.importer.GMQLImporter.{GMQLSource, GMQLTransformer}
import org.slf4j.LoggerFactory

/**
  * Created by Nacho on 10/13/16.
  */
class NULLTransformer extends GMQLTransformer {
  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * by receiving an original filename returns the new GDM candidate name.
    *
    * @param filename original filename
    * @return candidate names for the files derived from the original filename.
    */
  override def getCandidateNames(filename: String): List[String] = {
    List[String](filename)
  }

  /**
    * recieves .json and .bed.gz files and transform them to get metadata in .meta files and region in .bed files.
    *
    * @param source           source where the files belong to.
    * @param originPath       path for the  "Downloads" folder
    * @param destinationPath  path for the "Transformations" folder
    * @param originalFilename name of the original file .json/.gz
    * @param filename         name of the new file .meta/.bed
    * @return List(fileId, filename) for the transformed files.
    */
  override def transform(source: GMQLSource, originPath: String, destinationPath: String, originalFilename: String,
                filename: String): Unit = {
    val fileTransformationPath = destinationPath + File.separator + filename
    val fileDownloadPath = originPath + File.separator + originalFilename
    try {
      Files.copy(new File(fileDownloadPath), new File(fileTransformationPath))
      //here have to add the metadata of copy number and total copies
      logger.info("File: " + fileDownloadPath + " copied into " + fileTransformationPath)
    }
    catch {
      case e: IOException => logger.error("could not copy the file " +
        fileDownloadPath + " to " +
        fileTransformationPath, e)
    }
  }

}