package it.polimi.genomics.importer.GMQLImporter

/**
  * Created by Nacho on 10/13/16.
  * GMQLDownloader trait inherits the download method which is
  * supposed to be used with a specific loader.
  * ex: FTPDownloader should use TCGA2BEDInformation
  */
trait GMQLDownloader {
  /**
    * downloads the files from the source defined in the loader
    * into the folder defined in the loader
    * @param source contains specific download and sorting info.
    */
  def download(source: GMQLSource):Unit
}
