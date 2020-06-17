package org.example.ecommercerecommendation

import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.EmptyActualResult
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

case class DataSourceParams(appName: String) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    // create a RDD of (entityID, User)
    val usersRDD: RDD[(String, User)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "user"
    )(sc).map { case (entityId, properties) =>
      val user = try {
        User(address=properties.getOpt[String]("address"),
          field=properties.getOpt[String]("field")
        )
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" user ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, user)
    }.cache()

    // create a RDD of (entityID, Item)
    val itemsRDD: RDD[(String, Item)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "item"
    )(sc).map { case (entityId, properties) =>
      val item = try {
        // Assume categories is optional property of item.
        Item(categories = properties.getOpt[String]("categories"),
          descriptionRequire = properties.getOpt[String]("description_require"),
          city = properties.getOpt[String]("city"),
          skills = properties.getOpt[List[String]]("skills")
        )
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" item ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, item)
    }.cache()

    val eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("apply", "save")),
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("item")))(sc)
      .cache()

    val applyEventsRDD: RDD[ApplyEvent] = eventsRDD
      .filter { event => event.event == "apply" }
      .map { event =>
        try {
          ApplyEvent(
            user = event.entityId,
            item = event.targetEntityId.get,
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to ApplyEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }

    val saveEventsRDD: RDD[SaveEvent] = eventsRDD
      .filter { event => event.event == "save" }
      .map { event =>
        try {
          SaveEvent(
            user = event.entityId,
            item = event.targetEntityId.get,
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to SaveEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }

    new TrainingData(
      users = usersRDD,
      items = itemsRDD,
      applyEvents = applyEventsRDD,
      saveEvents = saveEventsRDD
    )
  }
}

case class User(address:Option[String],field:Option[String])

case class Item(categories: Option[String],descriptionRequire: Option[String],city:Option[String],skills:Option[List[String]])

case class ApplyEvent(user: String, item: String, t: Long)

case class SaveEvent(user: String, item: String, t: Long)

class TrainingData(
  val users: RDD[(String, User)],
  val items: RDD[(String, Item)],
  val applyEvents: RDD[ApplyEvent],
  val saveEvents: RDD[SaveEvent]
) extends Serializable {
  override def toString = {
    s"users: [${users.count()} (${users.take(2).toList}...)]" +
    s"items: [${items.count()} (${items.take(2).toList}...)]" +
    s"applyEvents: [${applyEvents.count()}] (${applyEvents.take(2).toList}...)" +
    s"saveEvents: [${saveEvents.count()}] (${saveEvents.take(2).toList}...)"
  }
}
