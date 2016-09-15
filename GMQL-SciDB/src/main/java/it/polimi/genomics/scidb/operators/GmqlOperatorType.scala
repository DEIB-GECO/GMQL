package it.polimi.genomics.scidb.operators

import it.polimi.genomics.scidbapi.schema.{Attribute, Dimension}


// -------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------


/**
  * Provide a specification for meta operators, providing
  * the required schema
  */
abstract class GmqlMetaOperator
  extends GmqlOperator
{

  /**
    * Returns the theoretic schema of the result, in order to verify it
    * with the actual schema obtained by the computation
    *
    * @return Theoretic schema
    */
  override final def schema : (List[Dimension], List[Attribute]) =
    (GDS.meta_dimensions_D, GDS.meta_attributes_A)

}



// -------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------



/**
  * Provide a specification for region operators, providing
  * the required schema
  */
abstract class GmqlRegionOperator
  extends GmqlOperator
{

  /**
    * Returns the theoretic schema of the result, in order to verify it
    * with the actual schema obtained by the computation
    *
    * @return Theoretic schema
    */
  override final def schema : (List[Dimension], List[Attribute]) =
    (GDS.regions_dimensions_D, List[Attribute]())

}



// -------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------



/**
  * Provide a specification for meta regions operators, providing
  * the required schema
  */
abstract class GmqlMetaJoinOperator
  extends GmqlOperator
{

  /**
    * Returns the theoretic schema of the result, in order to verify it
    * with the actual schema obtained by the computation
    *
    * @return Theoretic schema
    */
  override final def schema : (List[Dimension], List[Attribute]) =
    (GDS.metajoin_dimensions_D, GDS.metajoin_attributes_A)

}



// -------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------



/**
  * Provide a specification for meta regions operators, providing
  * the required schema
  */
abstract class GmqlMetaGroupOperator
  extends GmqlOperator
{

  /**
    * Returns the theoretic schema of the result, in order to verify it
    * with the actual schema obtained by the computation
    *
    * @return Theoretic schema
    */
  override final def schema : (List[Dimension], List[Attribute]) =
    (GDS.metagroup_dimensions_D, GDS.metagroup_attributes_A)

}

