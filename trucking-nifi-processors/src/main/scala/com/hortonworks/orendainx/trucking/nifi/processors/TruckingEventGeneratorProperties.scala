package com.hortonworks.orendainx.trucking.nifi.processors

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.util.StandardValidators

trait TruckingEventGeneratorProperties {
  val ExampleProperty =
    new PropertyDescriptor.Builder()
      .name("Example Property")
      .description("Whatever the property does")
      .required(true)
      .expressionLanguageSupported(true)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build

  lazy val properties = List(ExampleProperty)
}
