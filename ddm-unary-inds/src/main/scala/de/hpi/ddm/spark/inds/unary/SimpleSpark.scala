package de.hpi.ddm.spark.inds.unary

import java.io.File

import org.apache.spark.sql.SparkSession


object SimpleSpark extends App {
  override def main(args: Array[String]): Unit = {
    println(s"Started main")

    //ugly command line parsing
    import org.apache.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Option, Options, ParseException}

    val options = new Options
    val path = new Option("p", "path", true, "input folder path")
    options.addOption(path)
    val output = new Option("c", "cores", true, "cores")
    options.addOption(output)

    val parser = new DefaultParser
    val formatter = new HelpFormatter
    var cmd: CommandLine = null


    try {
      cmd = parser.parse(options, args)
    }
    catch {
      case e: ParseException =>
        System.out.println(e.getMessage)
        formatter.printHelp("utility-name", options)
        System.exit(1)
    }

    var inputPath = cmd.getOptionValue("input")
    if (inputPath == null) inputPath = "./TPCH"
    var coresString = cmd.getOptionValue("cores")
    if (coresString == null) coresString = "2"
    val cores = Integer.valueOf(coresString)


    //real Spark part
    val spark = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[" + cores.toString() + "]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") //turn off annoying logs


    val files = new File(inputPath).listFiles().map(f => f.getAbsolutePath).toList

    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println(s"Execution: ${t1 - t0} ms")
      result
    }

    time {
      Sindy.discoverINDs(files, spark)
    }

  }
}
