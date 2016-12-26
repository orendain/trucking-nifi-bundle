package com.hortonworks.orendainx.trucking.nifi.processors

import java.io._
import java.util.concurrent.atomic.AtomicReference

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.{ AbstractProcessor, Relationship }
import org.apache.nifi.processor.{ ProcessorInitializationContext, ProcessContext, ProcessSession }
import org.apache.nifi.annotation.behavior.{ ReadsAttribute, ReadsAttributes }
import org.apache.nifi.annotation.behavior.{ WritesAttribute, WritesAttributes }
import org.apache.nifi.annotation.documentation.{ CapabilityDescription, SeeAlso, Tags }
import org.apache.nifi.annotation.lifecycle.OnScheduled

import com.typesafe.config.ConfigFactory

@Tags(Array("example"))
@CapabilityDescription("An example processor")
@SeeAlso(Array())
@ReadsAttributes(Array(
  new ReadsAttribute(attribute = "", description = "")))
@WritesAttributes(Array(
  new WritesAttribute(attribute = "", description = "")))
class TruckingEventGenerator extends AbstractProcessor with TruckingEventGeneratorProperties
    with TruckingEventGeneratorRelationships {

  import scala.collection.JavaConverters._

  private[this] val className = this.getClass.getName

  private[this] lazy val config = ConfigFactory.load().getConfig(className)

  protected[this] override def init(context: ProcessorInitializationContext): Unit = {
  }

  override def getSupportedPropertyDescriptors(): java.util.List[PropertyDescriptor] = {
    properties.asJava
  }

  override def getRelationships(): java.util.Set[Relationship] = {
    relationships.asJava
  }

  @OnScheduled
  def onScheduled(context: ProcessContext): Unit = {
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
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
  }
}
