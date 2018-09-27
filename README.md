# Azure Databricks Samples

## Topic 1: Deployment of R models w/ Azure Databricks

If we are discussing a deployment architecture for ML batch scoring scenarios w/ R code, the core components of a deployable architecture could be:
  1. Azure Data Factory
  2. Azure Data Lake Storage
  3. Azure Databricks
  
 ![Databricks Batch Scoring Architecture](https://dm2304files.storage.live.com/y4mMC2EahSYn5bwTF3uwK6LoFAdtb8sSiLtyoOsrpZjU6DaN9UD_HEPZkZYcgkBKbVNrbQZcq_xLsosMim-AQn0M8pquiW73oZ3xOYNLBAoJZhJ5CSwF151fCFeY8yxkv9LRuckjQHMcxcJiTUladvfilRMST1K8N3XyOdgCOOyG3g3c3kRqYeT5wMH1_Z6fc-gwDN8GLXDrkg5z3mJ9jhG8Q/batch-databricks-architecture.png?psid=1&width=843&height=488)

### How to start deploying R/SparkR code in Databricks?

**Step 0:** This is a nice community post to read if SparkR is a novelty to you:
  - [10 things I wish someone had told me before I started using Apache SparkR]
 
*Important quote:*

- “The SparkR API presents a full R interface, supplemented with the {SparkR} package. As an experienced R user, you will be familiar with the R data.frame object. Here's the critical point - SparkR has its own DataFrame object, which is not the same thing as an R data.frame. You can convert between them easily (sometimes too easily), but you must respect which is which.”
 
**Step 1**: [Create an Azure Databricks Workspace]
 
**Step 2:** [Create an ADLS (Azure Data Lake Storage)]

- Obs.: Create the ADLS on the same region that you’ve provisioned Azure Databricks
 
**Step 3:** [Create a cluster inside Databricks]
 
**Step 4:** [Execute and understand this sample code] (`SparkR + ADLS.r`) . Tasks performed on this sample:

- ADLS (Azure Data Lake Storage Gen1) Mount for usage with R and SparkR
- Usage of Databricks dbutils library
- R and SparkR read/write taks
- `DataFrame`/`data.frame` mapping between R and SparkR
 
### Orchestrating Databricks batch scoring 
 
Here are some additional resources for understanding the orchestration of the R models execution:
- Orchestrating Databricks code execution via ADF: https://docs.microsoft.com/es-mx/azure/data-factory/transform-data-using-databricks-notebook
- Databricks linked service configuration for creating a new Databricks cluster (using ADF): https://docs.microsoft.com/es-mx/azure/data-factory/compute-linked-services#azure-databricks-linked-service
- Documentation of the Databricks Notebook Activity (using ADF): https://docs.microsoft.com/es-mx/azure/data-factory/transform-data-databricks-notebook

[10 things I wish someone had told me before I started using Apache SparkR]:https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/8599738367597028/1792412399382575/3601578643761083/latest.html
[Create an Azure Databricks Workspace]:https://docs.microsoft.com/es-mx/azure/azure-databricks/quickstart-create-databricks-workspace-portal
[Create a cluster inside Databricks]:https://docs.microsoft.com/es-mx/azure/azure-databricks/quickstart-create-databricks-workspace-portal#create-a-spark-cluster-in-databricks
[Create an ADLS (Azure Data Lake Storage)]:https://docs.microsoft.com/pt-br/azure/data-lake-store/data-lake-store-get-started-portal
[Execute and understand this sample code]:https://github.com/nansravn/Databricks101/blob/master/SparkR%20%2B%20ADLS.r
