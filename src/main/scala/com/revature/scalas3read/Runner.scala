package com.revature.scalas3read

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.IntegerType
import com.google.flatbuffers.Struct
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import scala.io.BufferedSource
import java.io.FileInputStream
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.round 
import java.util.Arrays
import java.sql

object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("scalas3read")
      // .master("local[4]")
      // .config("spark.debug.maxToStringFields", 100)
      .getOrCreate()

    // Reference: https://sparkbyexamples.com/spark/spark-read-text-file-from-s3/#s3-dependency
    val key = System.getenv(("AWS_ACCESS_KEY_ID"))
    val secret = System.getenv(("AWS_SECRET_ACCESS_KEY"))

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    // Read in a Census CSV gathered from https://data.census.gov/cedsci/table?q=dp05&g=0100000US.04000.001&y=2019&tid=ACSDT1Y2019.B01003&moe=false&hidePreview=true
    // This CSV was of 2019 1 year estimate for population in every state

    val censusData = spark.read
      .format("csv")
      .option("header", "true")
      .load("ACSDT1Y2019.B01003_data_with_overlays_2021-02-23T105100.csv")

    // Created a State Code list for easier joining with additional warc data. 
    val rawStateList = Seq(
      ("AL", "Alabama"), ("AK", "Alaska"), ("AZ", "Arizona"), ("AR", "Arkansas"), ("CA", "California"), ("CO", "Colorado"), ("CT", "Connecticut"), ("DE", "Delaware"), 
      ("DC", "District of Columbia"), ("FL", "Florida"), ("GA", "Georgia"), ("HI", "Hawaii"), ("ID", "Idaho"), ("IL", "Illinois"), ("IN", "Indiana"), ("IA", "Iowa"), 
      ("KS", "Kansas"), ("KY", "Kentucky"), ("LA", "Louisiana"), ("ME", "Maine"), ("MD", "Maryland"), ("MA", "Massachusetts"), ("MI", "Michigan"), ("MN", "Minnesota"), 
      ("MS", "Mississippi"), ("MO", "Missouri"), ("MT", "Montana"), ("NE", "Nebraska"), ("NV", "Nevada"), ("NH", "New Hampshire"), ("NJ", "New Jersey"), ("NM", "New Mexico"), 
      ("NY", "New York"), ("NC", "North Carolina"), ("ND", "North Dakota"), ("OH", "Ohio"), ("OK", "Oklahoma"), ("OR", "Oregon"), ("PA", "Pennsylvania"), ("RI", "Rhode Island"), 
      ("SC", "South Carolina"), ("SD", "South Dakota"), ("TN", "Tennessee"), ("TX", "Texas"), ("UT", "Utah"), ("VT", "Vermont"), ("VA", "Virginia"), ("WA", "Washington"), 
      ("WV", "West Virginia"), ("WI", "Wisconsin"), ("WY", "Wyoming"))

    val stateList = rawStateList.toDF("State Code", "State Name")

    // Combined the two dataFrames to get state codes assocaited with area name.

    val combinedCensusData = censusData.join(stateList, $"Geographic Area Name" === $"State Name")

    // combinedCensusData
    //   .select("State Name", "State Code", "Population Estimate Total")
    //   .show()
    
    //fetch_time BETWEEN TIMESTAMP '2020-03-01 00:00:00' AND TIMESTAMP '2020-03-31 23:59:59'

    // Read WET data from the Common Crawl s3 bucket
    // Since WET is read in as one long text file, use lineSep to split on each header of WET record
    val commonCrawl = spark.read.option("lineSep", "WARC/1.0").text(
      "s3://commoncrawl/crawl-data/CC-MAIN-2019-51/segments/1575541319511.97/wet/CC-MAIN-20191216093448-20191216121448-00559.warc.wet.gz"
    )
    .as[String]
    .map((str)=>{str.substring(str.indexOf("\n")+1)})
    .toDF("cut WET")

    // Splitting the header WARC data from the plain text content for WET files
    val cuttingCrawl = commonCrawl
      .withColumn("_tmp", split($"cut WET", "\r\n\r\n"))
      .select($"_tmp".getItem(0).as("WARC Header"), $"_tmp".getItem(1).as("Plain Text"))
  
    // Below is filtering for only Tech Jobs
    // First filter for URI's with career/job/employment
    // Second filter makes sure all results returned have keys words for tech jobs
    // This filters out non tech related jobs hat return from job in URI
    val filterCrawl = cuttingCrawl
      .filter($"WARC Header" rlike ".*WARC-Target-URI:.*career.*" 
        or ($"WARC Header" rlike ".*WARC-Target-URI:.*/job.*") 
        or ($"WARC Header" rlike ".*WARC-Target-URI:.*employment.*"))
      .filter($"Plain Text" rlike ".*Frontend.*" 
        or ($"Plain Text" rlike ".*Backendend.*") 
        or ($"Plain Text" rlike ".*Fullstack.*")
        or ($"Plain Text" rlike ".*Cybersecurity.*") 
        or ($"Plain Text" rlike ".*Software.*") 
        or ($"Plain Text" rlike ".*Computer.*"))
      .select($"WARC Header", $"Plain Text")
       

    // Turning Dataframe into RDD in order to get Key-Value pairs of occurrences of State Codes
    val sqlCrawl = filterCrawl
    .select($"Plain Text").as[String].flatMap(line => line.split(" ")).rdd

    val rddCrawl = sqlCrawl.map(word => (word, 1))
    .filter({case (key, value) => key.length < 3})
    .reduceByKey(_ + _)
    .toDF("State Code", "Tech Job Total")

    // Join earlier combinedCensusData Dataframe to rddCrawl Dataframe in order to determine 
    // question: "Where do we see relatively fewer tech ads proportional to population?"
    val combinedCrawl = rddCrawl.join(combinedCensusData,("State Code"))
    .withColumn("Tech Ads Proportional to Population", round(($"Tech Job Total" / $"Population Estimate Total" * 100) , 8))
    .select($"State Code", $"Geographic Area Name", $"Tech Job Total", $"Population Estimate Total", $"Tech Ads Proportional to Population")

    val s3OutputBucket = "s3://commoncrawlques1outputbucket/commoncrawl-demo-data"

    combinedCrawl.write.format("csv").mode("overwrite").save(s3OutputBucket)

    spark.close
  }

}