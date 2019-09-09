package it.polimi.genomics.compiler

import it.polimi.genomics.core.DataStructures.Instance

trait FederatedPolicyCompiler {

}

case class DistributedPolicyCompiler() extends FederatedPolicyCompiler

case class CentralizedPolicyCompiler(instance: Instance) extends FederatedPolicyCompiler

case class ExternalizedPolicyCompiler(instance: Instance) extends FederatedPolicyCompiler

