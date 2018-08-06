import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.log4j.{Level, Logger}

object SparkSQL1 {

  //Case Class for Holidays
  case class Holidays (UserID:Int,Country_Name_Dept:String,Country_Name_Arrival:String,modeOfTravel:String,Distance:Int, Year:Int)

  //Case Class for Transport Details
  case class Transport_Details(Transport_Mode:String,Transport_Exp:Int)

  //Case Class for User Details
  case class User_Details(UserID:Int,User_Name:String,Age:Int)

  def main(args:Array[String]): Unit = {
    
    //Let us create a spark session object
    //Create a case class globally to be used inside the main method
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL - I")
      .getOrCreate()

    // Removing all INFO logs in consol printing only result sets
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    import spark.implicits._
    //Read the Holiday Details from Local file

    val data = spark.sparkContext.textFile("C:\\Users\\Shruthi\\Downloads\\S20_Dataset_Holidays.txt")
    //Create Holdiays DF

    val holidaysDF = data.map(_.split(",")).map(x=>Holidays(UserID = x(0).toInt,Country_Name_Dept = x(1),Country_Name_Arrival = x(2),
      modeOfTravel = x(3),Distance = x(4).toInt,Year = x(5).toInt)).toDF()

    //Printing data of Holidays DF
    holidaysDF.show()

    //Create Transport Details DF by loading the Transport_Details file
    val transportDetailsDF = spark.sparkContext.textFile("C:\\Users\\Shruthi\\Downloads\\S20_Dataset_Transport.txt").
      map(_.split(",")).map(x=>Transport_Details(Transport_Mode = x(0),Transport_Exp = x(1).toInt)).toDF()

    //Printing data of Transport Mode DF
    transportDetailsDF.show()

    //Create USer Details DF by loading the User file
    val userDetailsDF = spark.sparkContext.textFile("C:\\Users\\Shruthi\\Downloads\\S20_Dataset_User_details.txt").
      map(_.split(",")).map(x=>User_Details(UserID = x(0).toInt,User_Name = x(1),Age = x(2).toInt)).toDF()

    //Printing data of Transport Mode DF
    userDetailsDF.show()

    //Task 1
    //1) What is the distribution of the total number of air-travelers per year

    //This is by using filter and group by operations on DataFrame
    holidaysDF.filter("modeOfTravel='airplane'").groupBy("Year").count().show()

    //Below approach is by using SQL in spark
    holidaysDF.createOrReplaceTempView("Holiday_Data")
    println("Using SQL & Temp View")
    spark.sql("Select year, count(Year) a from Holiday_Data where modeOfTravel='airplane' group By Year ").show()

    //Task 1
    //2) What is the total air distance covered by each user per year

    //creating Or replacing the view
    userDetailsDF.createOrReplaceTempView("Users_Data")

    //Approach : By joinging two DFs
    println("Below Result is after joining two Data frames")
    holidaysDF.as('HD).join(userDetailsDF.as('UD),$"UD.UserID"===$"HD.UserID")
      .groupBy("HD.UserID","HD.Year","UD.User_Name").sum("Distance").show()



    //Task 1
    //3) Which user has travelled the largest distance till date
    //Approach : Using Spark SQL Operations
    val result3 = holidaysDF.as('HD).join(userDetailsDF.as('UD),$"UD.UserID"===$"HD.UserID").
      groupBy("HD.UserID","HD.Year","UD.User_Name").sum("Distance")
      .withColumnRenamed("sum(Distance)","MaxDistance")
      .sort(desc("MaxDistance")).take(1).mkString(",")
    println(result3)

    ////Task 1
    // 4) What  is the most preferred destination for all users.

    //Approach : Using Spark SQL Operations
    holidaysDF.groupBy("Country_Name_Arrival").count()
      .sort(desc("count")).show(1)

    //Task 1
    // 5) Which route is generating the most revenue per year

    //Approach Using Spark SQL Operations
    //First create a new DF where two columns Dept Country and Arrival country should be kep in one column to get distinct routes
    val routesDF= holidaysDF.withColumn("Route",struct("Country_Name_Dept","Country_Name_Arrival")).toDF()
    routesDF.as('HD).join(transportDetailsDF.as('TD),$"TD.Transport_Mode"===$"HD.modeOfTravel")
      .groupBy("HD.Route").sum("Transport_Exp").
      withColumnRenamed("sum(Transport_Exp)","Total_Exp").sort(desc("Total_Exp")).show(1)

    //Task 1
    // 6) What is the total amount spent by every user on air-travel per year
    //Approach : Using spark SQL operations
    holidaysDF.as('HD).join(userDetailsDF.as('UD),$"UD.UserID"===$"HD.UserID")
      .join(transportDetailsDF.as("TD"),$"HD.modeOfTravel"===$"TD.Transport_Mode")
      .groupBy("UD.UserID","UD.User_Name","HD.Year").sum("Transport_Exp")
      .sort("UserID","Year")show()

    //Task 1
    // 7) Considering age groups of < 20 , 20-35, 35 > ,Which age group is travelling the most
    //every year.

    //another Approach of Case Statement
    holidaysDF.as('HD).join(userDetailsDF.as('UD),$"UD.UserID"===$"HD.UserID")
      .select(when($"Age"<20,"LessThan20").
        when($"Age" > 20 && $"Age"<35,"Between20And35").
        when($"Age">35,"Above35").alias("AgeGroup")).
      groupBy("AgeGroup").count().sort(desc("count")).show(1)


  }

}
