package com.hortonworks.orendainx.trucking.nifi.processors

import java.io._
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicReference

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.io.{InputStreamCallback, OutputStreamCallback}
import org.apache.nifi.processor.{AbstractProcessor, Relationship}
import org.apache.nifi.processor.{ProcessContext, ProcessSession, ProcessorInitializationContext}
import org.apache.nifi.annotation.behavior._
import org.apache.nifi.annotation.documentation.{CapabilityDescription, SeeAlso, Tags}
import org.apache.nifi.annotation.lifecycle.{OnRemoved, OnScheduled, OnShutdown}
import com.typesafe.config.ConfigFactory
import com.hortonworks.orendainx.trucking.simulator.NiFiSimulator
import akka.actor.Inbox
import com.hortonworks.orendainx.trucking.shared.models.Event
import com.hortonworks.orendainx.trucking.simulator.coordinators.ManualCoordinator
import com.hortonworks.orendainx.trucking.simulator.transmitters.AccumulateTransmitter
import com.typesafe.scalalogging.Logger
import org.apache.nifi.logging.ComponentLog

import scala.concurrent.duration._

@Tags(Array("trucking", "event", "data" , "generator", "simulator", "iot"))
@CapabilityDescription("Generates event data for a trucking application.")
@WritesAttributes(Array(
  new WritesAttribute(attribute = "test.1", description = "Test 1"),
  new WritesAttribute(attribute = "test.2", description = "Test 2"),
  new WritesAttribute(attribute = "test.3", description = "Test 3")
))
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@TriggerSerially
class GetTruckingEvent extends AbstractProcessor with GetTruckingEventProperties
    with GetTruckingEventRelationships {

  import scala.collection.JavaConverters._

  private var log: ComponentLog = _

  private lazy val config = ConfigFactory.load()
  private lazy val simulator = NiFiSimulator()
  private lazy val inbox = Inbox.create(simulator.system)

  override def init(context: ProcessorInitializationContext): Unit = {
    inbox.send(simulator.coordinator, ManualCoordinator.Tick)
    log = context.getLogger
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {

    //simulator.coordinator ! ManualCoordinator.Tick

    // Fetch the results to be retrieved by onTrigger's next invocation
    inbox.send(simulator.transmitter, AccumulateTransmitter.Fetch)

    // TODO: don't wait an entire second, should return immediately (ideally)
    log.debug(s"Attempting to receive")
    val lst = inbox.receive(1.second)
    log.debug(s"Received: $lst")

    lst match {
      case events: List[_] =>
        events.foreach { e =>
          val event = e.asInstanceOf[Event]
          //log.debug(s"This ev: ${event.asInstanceOf[Event]}")
          log.debug(s"This ev: ${event}")
          var flowFile = session.create()
          flowFile = session.putAttribute(flowFile, "testAttr1", "1")
          flowFile = session.putAttribute(flowFile, "testAttr2", "2")

          flowFile = session.write(flowFile, new OutputStreamCallback {
            override def process(outputStream: OutputStream) = {
              outputStream.write("outputText".getBytes(StandardCharsets.UTF_8))
            }
          })

          session.getProvenanceReporter.route(flowFile, RelSuccess)
          //session.getProvenanceReporter.receive(flowFile, "What?")
          session.transfer(flowFile, RelSuccess)
          session.commit()
        }
    }

    inbox.send(simulator.coordinator, ManualCoordinator.Tick)

    //simulator.coordinator ! ManualCoordinator.Tick

    // Fetch the results to be retrieved by onTrigger's next invocation
    //simulator.transmitter ! AccumulateTransmitter.Fetch
  }

  /*def NotonTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    val flowFile = session.get

    Option(flowFile) match {
      case Some(f) => {
        val property =
          context.getProperty(ExampleProperty)
            .evaluateAttributeExpressions(flowFile)
            .getValue

        val content = new AtomicReference[String]
        session.read(flowFile, new InputStreamCallback {
          override def process(in: InputStream): Unit = {
            try {
              //http://stackoverflow.com/questions/309424/read-convert-an-inputstream-to-a-string
              //val s = IOUtils.toString(in)
              val s = "TempString"
              content.set(s)
            } catch {
              case t: Throwable =>
                getLogger().error(t.getMessage, t)
                session.transfer(flowFile, RelFailure)
            }
          }
        })
      }
      case _ =>
        getLogger().warn("FlowFile was null")
    }

    session.transfer(flowFile, RelSuccess)
  }*/

  override def getSupportedPropertyDescriptors(): java.util.List[PropertyDescriptor] =
    properties.asJava

  override def getRelationships(): java.util.Set[Relationship] =
    relationships.asJava

  @OnRemoved
  @OnShutdown
  def cleanup(): Unit = {
    // Stop the simulation
    simulator.stop()
  }
}
