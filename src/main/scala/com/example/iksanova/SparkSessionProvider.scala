package com.example.iksanova

import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {
  def sparkSession: SparkSession
}

class DefaultSparkSessionProvider(appName: String) extends SparkSessionProvider {
  override lazy val sparkSession: SparkSession =
    SparkSession.builder
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
}

trait SparkSessionProviderComponent {
  def sparkSessionProvider: SparkSessionProvider
}

