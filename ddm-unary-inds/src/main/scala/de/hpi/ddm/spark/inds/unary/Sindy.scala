package de.hpi.ddm.spark.inds.unary

import org.apache.spark.sql.SparkSession

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    val tables = inputs.map(i =>
      spark.read
        .option("sep", ";")
        .option("inferSchema", "true")
        .option("header", "true")
        .csv(i)
    )

    val valueSets = tables.flatMap(df =>
      df.columns.map(col =>
        df.select(col).rdd.map(r => (r(0).toString, Set[String](col)))
      )
    ).reduce(_ union _).reduceByKey(_ ++ _).values.distinct()
    //1. map each entry in each table to (value, [columnName]) tuple
    //2. group by value, add possible columns for value to [columnName] set
    //3. filter out duplicate values

    //create inclusionLists
    val includes = valueSets.flatMap(
      set =>
        set.map(column =>
          (column, set - column)
        )
    )
    //inclusionLists.foreach(i => println(i))

    //intersect includes and keep only matches
    val aggregateSets = includes.reduceByKey(_.intersect(_))
      .filter(_._2.nonEmpty).sortByKey(numPartitions = 1)

    //print results
    aggregateSets.foreach(row =>
      println(row._1 + " < " + row._2.mkString(", "))
    )

  }
}
