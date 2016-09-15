package it.polimi.genomics.core.DataStructures.Builtin

import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToMeta


/**
 * Created by pietro on 20/07/15.
 */
trait ExtendFunctionFactory {

  /**
   * provides a nullary map aggregation function
   * @param name name of the function
   * @return the aggregate function
   */
  def get(name:String, out_name : Option[String]):RegionsToMeta

  /**
   * provides a unary map aggregation function
   * @param name name of the function
   * @param position the position of the field which is the input of the function
   * @param out_name optionally the name of the new field in the schema
   * @return the aggregate function, configured to use the provided field
   */
  def get(name:String, position:Int, out_name : Option[String]):RegionsToMeta

}