package it.polimi.genomics.importer.DefaultImporter
import java.io.File

import com.google.common.io.Files
import it.polimi.genomics.importer.GMQLImporter.GMQLDataset
import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION

/**
  * Created by nachon on 12/13/16.
  */
object schemaFinder {
  /**
    * Puts the dataset schema inside the given folder (meant to be the transformations one).
    * schema is given the dataset name as name and .schema as extension.
    * @param rootFolder root working directory.
    * @param dataset GMQL dataset.
    * @param outputFolder folder where to put the schema.
    */
  def downloadSchema(rootFolder: String, dataset: GMQLDataset, outputFolder: String): Boolean ={
    val outputPath = outputFolder+File.separator+dataset.name+".schema"
    dataset.schemaLocation match{
      case SCHEMA_LOCATION.HTTP=>
        val downloader = new HTTPDownloader()
        if(downloader.urlExists(dataset.schemaUrl)) {
          downloader.downloadFileFromURL(dataset.schemaUrl, outputPath)
          true
        }
        else false
      case SCHEMA_LOCATION.LOCAL =>
        if(new File(rootFolder+File.separator+dataset.schemaUrl).exists()) {
          Files.copy(new File(rootFolder + File.separator + dataset.schemaUrl), new File(outputPath))
          true
        }
        else false
    }
  }
}
