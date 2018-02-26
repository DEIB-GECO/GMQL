package it.polimi.genomics.core.DataStructures.MetaGroupByCondition

import it.polimi.genomics.core.DataStructures.MetaJoinCondition.AttributeEvaluationStrategy

/**
 * specify the groupBy condition which is used by the COVER statement
 *
 * @param attributes is the list of attributes name to be used for the grouping.
 */
case class MetaGroupByCondition(attributes : List[AttributeEvaluationStrategy])
