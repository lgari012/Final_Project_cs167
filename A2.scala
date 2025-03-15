package edu.ucr.cs.cs167.lgall045

import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.sql.SparkSession
import edu.ucr.cs.bdlab.beast._

object A2 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)


    val crimes: String = args(0)
    val zipcodes: String = args(1)
    val outputFile = args(2)

    try {
      // Load the crimes DataFrame
      val crimesDF = sparkSession.read.parquet(crimes)
      crimesDF.createOrReplaceTempView("crimes")

      // Load the ZipCodes Dataframe
      sparkSession.read.format("shapefile").load(zipcodes).createOrReplaceTempView("counties")

      // Create the aggregate query for counting the crimes
      val query_aggregate =
        """
          |SELECT ZIPCode as zipcode, COUNT(*) AS crime_count
          |FROM crimes
          |GROUP BY ZIPCode
        """.stripMargin

      // Execute the query
      val groupedDF = sparkSession.sql(query_aggregate)
      groupedDF.createOrReplaceTempView("grouped")

      // Create the query to join with the ZipCodes DataFrame
      val join_query =
        """
          |SELECT g.zipcode as zipcode, g.crime_count as crime_count, c.geometry as geometry
          |FROM grouped g, counties c
          |WHERE g.zipcode = c.ZCTA5CE10
        """.stripMargin

      // Execute the query
      val resultDF = sparkSession.sql(join_query)

      // Save the shapefile
      resultDF.coalesce(1).toSpatialRDD.saveAsShapefile(outputFile)

    } finally {
      sparkSession.stop()
    }
  }
}