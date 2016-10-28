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
  * @param parameters list with parameters
  */
case class GMQLDataset(
                   outputFolder:String,
                   schema:String,
                   schemaLocation: SCHEMA_LOCATION.Value,
                   parameters: Seq[(String,String)]
                   ) {
}
