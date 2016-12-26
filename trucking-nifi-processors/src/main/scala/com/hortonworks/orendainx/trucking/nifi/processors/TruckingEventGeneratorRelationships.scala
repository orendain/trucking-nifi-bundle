package com.hortonworks.orendainx.trucking.nifi.processors

import org.apache.nifi.processor.Relationship

trait TruckingEventGeneratorRelationships {
  val RelSuccess =
    new Relationship.Builder()
      .name("success")
      .description("""
        Any FlowFile that is successfully transferred is routed to this relationship
      """.trim)
      .build

  val RelFailure =
    new Relationship.Builder()
      .name("failure")
      .description("""
          Any FlowFile that fails to be transferred is routed to this relationship
      """.trim)
      .build

  lazy val relationships = Set(RelSuccess, RelFailure)
}
