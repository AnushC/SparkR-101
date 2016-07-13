#Program to connect an application running on a standalone client 
#machine to the Spark cluster & utilizing HDFS files via SparkR
#This is an adaptation from 2 blogposts on RStudio + SparkR. 
#http://blog.danielemaasit.com/2015/07/26/installing-and-starting-sparkr-locally-on-windows-8-1-and-rstudio/
#https://www.linkedin.com/pulse/running-your-r-analysis-spark-cluster-rutger-de-graaf

#The environment was setup on a Windows Server 2012 R2 machine (Yes, I was given Windows server to work with..don't judge)
#So, Hadoop was setup on Windows first, because - HDFS, baby!Spark was installed next. 
#Current program runs from a remote machine(laptop) running RStudio and SparkR makes the connection to the server
#running Spark on Hadoop and thus establishes a 3 tier approach scenario.
#Also running the RStudio progarm from a Windows laptop(sigh)

# Set the system environment variables

#For Spark - library 
Sys.setenv(SPARK_HOME = "C:/spark-1.6.2-bin-hadoop2.6")
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))

#For Hadoop
Sys.setenv(YARN_CONF_DIR = "C:/fresh-hadoop-2.6.0/etc/hadoop")
Sys.setenv(HADOOP_CONF_DIR = "C:/fresh-hadoop-2.6.0/etc/hadoop")
Sys.setenv(HADOOP_HOME = "C:/fresh-hadoop-2.6.0\bin")

#load the SparkR library
library(SparkR)

# Create a spark context and a SQL context - to make it run via Spark
sc <- sparkR.init(master = "spark://XX.XX.XX.XX:7077", appName="SparkR")

#To connect it to YARN - if you uncomment this you can see that the application runs on Hadoop/ view using UI
#Use either Spark or YARN - choosing Spark master - hence, commented this line out
#sc <- sparkR.init(master = "yarn-client", appName="SparkR")

sqlContext <- sparkRSQL.init(sc)

#tryme
people = read.df(sqlContext, "hdfs://XX.XX.XX.XX:19000/usr/data/gn/people.json",source = "json")

##Some Spark functions
printSchema(people)

# Register this DataFrame as a table.
registerTempTable(people, "people")

# SQL statements can be run by using the sql methods provided by sqlContext
teenagers <- sql(sqlContext, "SELECT name FROM people WHERE age >= 13 AND age <= 19")

# Call collect to get a local data.frame
teenagersLocalDF <- collect(teenagers)

# Print the teenagers in our dataset 
print(teenagersLocalDF)

# Stop the SparkContext now
sparkR.stop()

# The application ends