package it.polimi.genomics.importer.GMQLImporter

/**
  * Created by Nacho on 10/13/16.
  * once the files are downloaded, sorter using a loader
  * will check how to sort and insert them into the GMQLRepository
  * every sorter uses specific loader
  * ex: TCGA2BEDSorter should use TCGA2BEDSorter
  */
trait GMQLTransformer {
  /**
    * using the information in the loader should convert the downloaded files
    * into data and metadata as specified in GDM
    * @param source contains specific download and sorting info.
    */
  def transform(source: GMQLSource):Unit

  /**
    * using information in the loader should arrange the files into a single folder
    * where data and metadata are paired as (file,file.meta) and should put also
    * the schema file inside the folder.
    * @param source contains specific download and sorting info.
    */
  def organize(source: GMQLSource):Unit
}
