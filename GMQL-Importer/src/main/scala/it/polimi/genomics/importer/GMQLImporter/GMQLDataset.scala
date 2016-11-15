package it.polimi.genomics.importer.GMQLImporter

import it.polimi.genomics.importer.GMQLImporter.utils.SCHEMA_LOCATION

/**
  * Created by Nacho on 10/17/16.
  */
/**
  * represents a dataset from a source
  * @param outputFolder working subdirectory for the dataset
  * @param schema .schema file location
  * @param schemaLocation indicates if the schema is on local or remote location
  * @param downloadEnabled indicates whether download or not the datasets.
  * @param transformEnabled indicates whether transform or not the datasets.
  * @param loadEnabled indicates whether load or not the datasets.
  * @param parameters list with parameters
  */
case class GMQLDataset(
                        name:String,
                        outputFolder:String,
                        schema:String,
                        schemaLocation: SCHEMA_LOCATION.Value,
                        downloadEnabled: Boolean,
                        transformEnabled: Boolean,
                        loadEnabled: Boolean,
                        parameters: Seq[(String,String)]
                      ) {
}
