package com.hortonworks.orendainx.trucking.nifi.processors

import org.apache.nifi.processor.Relationship

trait GetTruckingEventRelationships {
  val RelSuccess = new Relationship.Builder()
    .name("success")
    .description("All generated events are routed to this relationship.")
    .build

  lazy val relationships = Set(RelSuccess)
}
