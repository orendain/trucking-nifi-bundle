package com.hortonworks.orendainx.trucking.nifi.processors

import java.io._
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicReference

import com.hortonworks.orendainx.trucking.shared.models.{TrafficData, TruckEvent, TruckingData}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.io.{InputStreamCallback, OutputStreamCallback}
import org.apache.nifi.processor.{AbstractProcessor, Relationship}
import org.apache.nifi.processor.{ProcessContext, ProcessSession, ProcessorInitializationContext}
import org.apache.nifi.annotation.behavior._
import org.apache.nifi.annotation.documentation.{CapabilityDescription, SeeAlso, Tags}
import org.apache.nifi.annotation.lifecycle.{OnRemoved, OnScheduled, OnShutdown}
import com.typesafe.config.ConfigFactory
import org.apache.nifi.logging.ComponentLog
import com.hortonworks.orendainx.trucking.simulator.NiFiSimulator
import com.hortonworks.orendainx.trucking.simulator.coordinators.ManualCoordinator

import scala.concurrent.duration._

/**
  * @author Edgar Orendain <edgar@orendainx.com>
  */
@Tags(Array("trucking", "event", "data" , "generator", "simulator", "iot"))
@CapabilityDescription("Generates event data for a trucking application.")
@WritesAttributes(Array(
  new WritesAttribute(attribute = "Data Type", description = "The type of TruckingData this flowfile holds (i.e. \"TruckEvent\", \"TrafficData\").")
))
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@TriggerSerially
class GetTruckingEvent extends AbstractProcessor with GetTruckingEventProperties
    with GetTruckingEventRelationships {

  import scala.collection.JavaConverters._

  private var log: ComponentLog = _

  private lazy val config = ConfigFactory.load()
  private lazy val simulator = NiFiSimulator()

  override def init(context: ProcessorInitializationContext): Unit = {
    simulator.coordinator ! ManualCoordinator.Tick
    log = context.getLogger
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {

    //simulator.coordinator ! ManualCoordinator.Tick

    // TODO: don't wait an entire second, should return immediately (ideally)
    log.debug(s"Attempting to receive")
    //val lst = simulator.inbox.receive(1.second)
    val lst = simulator.fetch()
    log.debug(s"Received: $lst")

    //lst match {
    lst.foreach { event =>
      //case events: List[_] =>
        //events.foreach { e =>
          //val event = e.asInstanceOf[TruckingData]
          /*
          log.debug(s"This e: ${e}")
          val event = e match {
            case ev: TruckEvent => ev
            case ev: TrafficData => ev
          }*/
          //log.debug(s"This ev: ${event.asInstanceOf[Event]}")
          log.debug(s"This ev: ${event}")
          var flowFile = session.create()
          flowFile = session.putAttribute(flowFile, "Data Type", event.name)

          flowFile = session.write(flowFile, new OutputStreamCallback {
            override def process(outputStream: OutputStream) = {
              outputStream.write(event.toCSV.getBytes(StandardCharsets.UTF_8))
            }
          })

          session.getProvenanceReporter.route(flowFile, RelSuccess)
          //session.getProvenanceReporter.receive(flowFile, "What?")
          session.transfer(flowFile, RelSuccess)
          session.commit()
        //}
    }
    //}

    simulator.coordinator ! ManualCoordinator.Tick

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
