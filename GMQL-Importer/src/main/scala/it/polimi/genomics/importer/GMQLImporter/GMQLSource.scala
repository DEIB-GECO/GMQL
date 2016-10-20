package it.polimi.genomics.importer.GMQLImporter

/**
  * Created by Nacho on 10/13/16.
  * Information is a container for the info stored in the xml config
  * file and every specific implementation stores needed information
  * loads specific information from the "source" node defined
  * in the xml file into the loader.
  * for the corresponding GMQLDownloader and Sorter.
  *
  * @param url source url
  * @param outputFolder working directory for the source
  * @param gmqlUser user for whom datasets are
  * @param downloader package and name of the downloader to be used for this source
  * @param download indicates whether download or not the datasets.
  * @param transformer package and name of the transformer to be used for this source
  * @param transform indicates whether transform or not the datasets.
  * @param load indicates whether load or not the datasets.
  * @param datasets datasets to be downloaded/transformed/loaded
  */
case class GMQLSource(
                        name:String,
                        url:String,
                        outputFolder:String,
                        gmqlUser:String,
                        downloader:String,
                        download: Boolean,
                        transformer:String,
                        transform: Boolean,
                        load: Boolean,
                        parameters: Seq[(String,String)],
                        datasets:Seq[GMQLDataset]
                      ) {
}
