# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks R Example

# COMMAND ----------

# MAGIC %md
# MAGIC Code Sample by ***Renan Vilas Novas on Sept 26, 2018***.
# MAGIC 
# MAGIC Tasks performed on this code:
# MAGIC - ADLS Mount for usage with R and SparkR
# MAGIC - Usage of Databricks **dbutils** library
# MAGIC - R and SparkR read/write taks
# MAGIC - `DataFrame`/`data.frame` mapping between R and SparkR

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# MAGIC %md
# MAGIC This is the documentation that explains how to mount an ADLS using Databricks: [link]
# MAGIC 
# MAGIC #### Step-by-Step:
# MAGIC 1. Follow this [Service-to-Service authentication tutorial] to get the AD Web App credential:
# MAGIC 
# MAGIC 2. You need to execute this python code to mount the ADLS:
# MAGIC 
# MAGIC     ```
# MAGIC     %python
# MAGIC 
# MAGIC     configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
# MAGIC                 "dfs.adls.oauth2.client.id": "<your-service-client-id>",
# MAGIC                 "dfs.adls.oauth2.credential": "<your-service-credentials>",
# MAGIC                 "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/<your-directory-id>/oauth2/token"}
# MAGIC 
# MAGIC     dbutils.fs.mount(
# MAGIC        source = "adl://<your-data-lake-store-account-name>.azuredatalakestore.net/<your-directory-name>",
# MAGIC        mount_point = "/mnt/<mount-name>",
# MAGIC        extra_configs = configs)
# MAGIC     ```
# MAGIC 
# MAGIC 3. These 3 values below you can get in the [Service-to-Service authentication tutorial]:
# MAGIC   - `your-service-client-id` is the application ID
# MAGIC   - `your-service-credentials` is the authentication key
# MAGIC   - `your-directory-id` is the tenant ID
# MAGIC   
# MAGIC 4. These other values also need to be replaced:
# MAGIC   - `your-data-lake-store-account-name` is the name of your ADLS (you can check on the Azure Portal)
# MAGIC   - `your-directory-name` is the path to a specific folder inside your ADLS, this path can be empty, redirecting you to the root of the ADLS
# MAGIC   - `mount-name` is an arbitrary name that will be used to mount the ADLS path on Databricks (choose any name here)
# MAGIC   
# MAGIC [link]:https://docs.databricks.com/spark/latest/data-sources/azure/azure-datalake.html#mount-a-data-lake-store
# MAGIC [Service-to-Service authentication tutorial]:https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-service-to-service-authenticate-using-active-directory

# COMMAND ----------

# MAGIC %md
# MAGIC #### ADLS mount example:

# COMMAND ----------

# MAGIC %python
# MAGIC configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
# MAGIC            "dfs.adls.oauth2.client.id": "79590097-2d5f-4126-9708-929bcc1*****", # REPLACE THIS ARGUMENT
# MAGIC            "dfs.adls.oauth2.credential": "3pe/HLJobJt+a8c3vwrAo54h55rOTaBfvfxb9H3*****", # REPLACE THIS ARGUMENT
# MAGIC            "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd01*****/oauth2/token"} # REPLACE THIS ARGUMENT
# MAGIC 
# MAGIC dbutils.fs.mount(
# MAGIC   source = "adl://databricksocptt.azuredatalakestore.net/",
# MAGIC   mount_point = "/mnt/adls/",
# MAGIC   extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC Here you can check everything that is inside the source folder mapped from ADLS:

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adls/

# COMMAND ----------

# MAGIC %md
# MAGIC Assuming that the source `adl://databricksocptt.azuredatalakestore.net/` is empty, let's create a new folder on ADLS for storing the raw data:

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.mkdirs("dbfs:/mnt/adls/raw")

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can check that an empty folder `adl://databricksocptt.azuredatalakestore.net/raw/` was created inside ADLS:

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adls/

# COMMAND ----------

# MAGIC %md
# MAGIC Let's save the **Iris** dataset as **test01.csv** using the `write.csv` R function.

# COMMAND ----------

iris

# COMMAND ----------

write.csv(iris, "/dbfs/mnt/adls/raw/test01.csv", row.names=FALSE, fileEncoding='utf-8')

# COMMAND ----------

# MAGIC %md
# MAGIC Reading again the **test01.csv** file

# COMMAND ----------

defaultDF <- read.csv("/dbfs/mnt/adls/raw/test01.csv", header = TRUE)

# COMMAND ----------

dim(defaultDF)

# COMMAND ----------

head(defaultDF,3)

# COMMAND ----------

class(defaultDF)

# COMMAND ----------

levels(iris$Species)

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's read the same file using the `read.df` SparkR function:

# COMMAND ----------

require(SparkR)
sparkDF <- read.df("dbfs:/mnt/adls/raw/test01.csv", source = "com.databricks.spark.csv", header="true", inferSchema = "true")

# COMMAND ----------

sparkDF

# COMMAND ----------

class(sparkDF)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's run some simple R code to manipulate the defaultDF `data.frame`:

# COMMAND ----------

defaultDF["Petal.Width.Sqrt"]= (defaultDF$Petal.Width)**(1/2)

# COMMAND ----------

c(head(defaultDF, 1))

# COMMAND ----------

defaultDF[,"Petal.Width.Sqrt"]

# COMMAND ----------

# MAGIC %md
# MAGIC Let's save the `data.frame` object (with the new `Petal.Width.Sqrt` column) inside another folder on ADLS.
# MAGIC 
# MAGIC For that purpose we will create a new folder on ADLS, for saving the pre-processed files.

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.mkdirs("dbfs:/mnt/adls/processed")

# COMMAND ----------

write.csv(defaultDF, "/dbfs/mnt/adls/processed/test01-processed.csv", row.names=FALSE, fileEncoding='utf-8')

# COMMAND ----------

# MAGIC %md
# MAGIC Converting the `data.frame` object in a Spark `DataFrame` and saving the result into **sparkDF**:

# COMMAND ----------

sparkDF <- as.DataFrame(defaultDF)

# COMMAND ----------

print(dim(sparkDF))
sparkDF

# COMMAND ----------

# MAGIC %md
# MAGIC Executing some simple SparkR code to manipulate the sparkDF `DataFrame`:

# COMMAND ----------

head(summarize(groupBy(sparkDF, sparkDF$Species), count = n(sparkDF$Species)))

# COMMAND ----------

sparkDF <- filter(sparkDF, sparkDF$Species!='versicolor')

# COMMAND ----------

head(summarize(groupBy(sparkDF, sparkDF$Species), count = n(sparkDF$Species)))

# COMMAND ----------

dim(sparkDF)

# COMMAND ----------

# MAGIC %md
# MAGIC The `as.data.frame()` function can also convert a Spark `DataFrame` object into a `data.frame`.

# COMMAND ----------

class(as.data.frame(sparkDF))

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's save the **sparkDF** Spark `DataFrame` as a `parquet`:

# COMMAND ----------

write.df(sparkDF, path = "dbfs:/mnt/adls/processed/test01.parquet", source = "parquet", mode = "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC Checking the new files saved on ADLS:

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adls/processed/

# COMMAND ----------

# MAGIC %md
# MAGIC [Here] you can learn more about `parquet` files.
# MAGIC 
# MAGIC Deleting all the created files/folders with `dbutils.fs.rm`:
# MAGIC [Here]:https://docs.databricks.com/spark/latest/data-sources/read-parquet.html

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.rm("dbfs:/mnt/adls/processed/", recurse = True)
# MAGIC dbutils.fs.rm("dbfs:/mnt/adls/raw/", recurse = True)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adls/

# COMMAND ----------

# MAGIC %md
# MAGIC Unmounting the ADLS:

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.unmount(mount_point = "/mnt/adls/")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Overview of [Databricks Notebooks].
# MAGIC   - Learn how to mix languages in a cell
# MAGIC   - Learn how to use `%fs`, `%md`, `%run`
# MAGIC   - Learn about notebook isolation
# MAGIC   - Understand version control w/ Databricks
# MAGIC     
# MAGIC 2. [dbutils] documentation
# MAGIC   - File system like commands to access files in DBFS 
# MAGIC   
# MAGIC 3. [Importing data]: documentation
# MAGIC   - Code samples for loading data in Databricks with several different languages/interfaces
# MAGIC 
# MAGIC 4. [Blob Storage mount] using Databricks
# MAGIC   - Documentation for using Blob Storage instead of ADLS
# MAGIC 
# MAGIC [dbutils]:https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html#access-dbfs-with-dbutils
# MAGIC [Databricks Notebooks]:https://docs.databricks.com/user-guide/notebooks/notebook-use.html
# MAGIC [Blob Storage mount]:https://docs.databricks.com/spark/latest/data-sources/azure/azure-storage.html#mount-an-azure-blob-storage-container
# MAGIC 
# MAGIC [Importing data]:https://docs.azuredatabricks.net/user-guide/importing-data.html#load-data
