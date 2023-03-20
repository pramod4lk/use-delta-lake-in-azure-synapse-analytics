from pyspark import spark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *

# One of the easiest ways to create a Delta Lake table is to save a dataframe in the delta format, 
# specifying a path where the data files and related metadata information for the table should be stored.

# Load a file into a dataframe
df = spark.read.load('abfss://staging@raritbiprod.dfs.core.windows.net/Data/Orders/2019.csv', format='csv')
df.limit(10)

# Save the dataframe as a delta table
delta_table_path = "/Delta"
df.write.format("delta").save(delta_table_path)

# Replace an existing Delta Lake table with the contents of a dataframe by using the overwrite mode
new_df.write.format("delta").mode("overwrite").save(delta_table_path)

# You can also add rows from a dataframe to an existing table by using the append mode.
new_rows_df.write.format("delta").mode("append").save(delta_table_path)

# You can use the DeltaTable object in the Delta Lake API, which supports update, delete, and merge operations.
from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, delta_table_path)

deltaTable.update(
    condition = "Category == 'Accessories'",
    set = { "Price": "Price * 0.9" })

# Querying a previous version of a table
df = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)

# Using timestamp
df = spark.read.format("delta").option("timestampAsOf", '2022-01-01').load(delta_table_path)

# Creating a catalog table from a dataframe
# Save a dataframe as a managed table
df.write.format("delta").saveAsTable("MyManagedTable")

## specify a path option to save as an external table
df.write.format("delta").option("path", "/mydata").saveAsTable("MyExternalTable")

# Creating a catalog table using SQL
spark.sql("CREATE TABLE MyExternalTable USING DELTA LOCATION '/mydata'")

# Alternatively you can use the native SQL support in Spark to run the statement.
CREATE TABLE MyExternalTable
USING DELTA
LOCATION '/mydata'

# Defining the table schema
CREATE TABLE ManagedSalesOrders
(
    Orderid INT NOT NULL,
    OrderDate TIMESTAMP NOT NULL,
    CustomerName STRING,
    SalesTotal FLOAT NOT NULL
)
USING DELTA

# Using the DeltaTableBuilder API
DeltaTable.create(spark) \
  .tableName("default.ManagedProducts") \
  .addColumn("Productid", "INT") \
  .addColumn("ProductName", "STRING") \
  .addColumn("Category", "STRING") \
  .addColumn("Price", "FLOAT") \
  .execute()