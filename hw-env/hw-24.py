from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("homework").getOrCreate()

# Specify the path to your CSV file on your local machine
file_path = "file:///Users/tasyahira/Documents/bootcamp-ds/homework-session-24/laptop_pricing_dataset.csv"


# Read the CSV data into a Spark DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Show the DataFrame
df.createOrReplaceTempView("laptop_data") #temporary view

###################
# Question Answer #
###################

# a
# How the average RAM GB per manufacturer ? which manufacturer has higher average RAM ?
# ANSWER: Razer (16.0)

df1 = spark.sql("select manufacturer, avg(ram_gb) as avg_ram from laptop_data group by 1 order by 2 desc")
df1.show()

#b
#Please calculate the average screen size per Screen Type, and order it by average from higher to lowest.
#ANSWER: On the screen-shoot folder

df2 = spark.sql("select screen, avg(screen_size_cm) as avg_screen_size from laptop_data group by 1 order by 2 desc")
df2.show()

#c
#Please do profiling by manufacturer and screen type, which manufacturer has
#more IPS Panel product, Full HD product, and so on by counting the record by those two columns.
#ANSWER: on the screen-shoot folder

df3 = spark.sql("select manufacturer, screen, count(*) as cnt from laptop_data group by 1,2 order by 3 desc")
df3.show()

# Stop the Spark session
spark.stop()
