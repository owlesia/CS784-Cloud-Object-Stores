// http://www.openkb.info/2021/02/how-to-generate-tpc-ds-data-and-run-tpc.html

import com.databricks.spark.sql.perf.tpcds.TPCDSTables
 
// Note: Declare "sqlContext" for Spark 2.x version
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 
val rootDir = "gs://784databucket/tpcds" // root directory of location to create data in.
 
val databaseName = "tpcds" // name of database to create.
val scaleFactor = "10" // scaleFactor defines the size of the dataset to generate (in GB).
val format = "parquet" // valid spark format like parquet "parquet".
// Run:
val tables = new TPCDSTables(sqlContext,
    dsdgenDir = "/home/shawgerj/tpcds-kit/tools", // location of dsdgen
    scaleFactor = scaleFactor,
    useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
    useStringForDate = false) // true to replace DateType with StringType
 
 
tables.genData(
    location = rootDir,
    format = format,
    overwrite = true, // overwrite the data that is already there
    partitionTables = true, // create the partitioned fact tables 
    clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files. 
    filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
    tableFilter = "", // "" means generate all tables
    numPartitions = 20) // how many dsdgen partitions to run - number of input tasks.
 
// Create the specified database
sql(s"create database $databaseName")
// Create metastore tables in a specified database for your data.
// Once tables are created, the current database will be switched to the specified database.
tables.createExternalTables(rootDir, "parquet", databaseName, overwrite = true, discoverPartitions = true)
// Or, if you want to create temporary tables
// tables.createTemporaryTables(location, format)
 
// For CBO only, gather statistics on all columns:
tables.analyzeTables(databaseName, analyzeColumns = true)  