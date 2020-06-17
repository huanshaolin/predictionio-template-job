package org.example.ecommercerecommendation

import org.apache.predictionio.controller.PPreparator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  override
  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    new PreparedData(
      users = trainingData.users,
      items = trainingData.items,
      applyEvents = trainingData.applyEvents,
      saveEvents = trainingData.saveEvents)
  }
}

class PreparedData(
  val users: RDD[(String, User)],
  val items: RDD[(String, Item)],
  val applyEvents: RDD[ApplyEvent],
  val saveEvents: RDD[SaveEvent]
) extends Serializable
