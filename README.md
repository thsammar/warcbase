Warcbase
========

Warcbase is an open-source platform for managing web archives built on HBase. The platform provides a flexible data model for storing and managing raw content as well as
metadata and extracted knowledge. Tight integration with Hadoop provides powerful tools for analytics and data processing.

Getting Started
---------------

Once you check out the repo, build WarcBase:

```
mvn clean package appassembler:assemble
```

Ingesting Content
-----------------

Ingesting archive content from ARC or WARC files:

```
$ setenv CLASSPATH_PREFIX "/etc/hbase/conf/"
$ sh target/appassembler/bin/IngestWarcFiles -dir /path/to/warc/ -name archive_name -create
```

Command-line options:

+ Use the `-dir` option to specify directory containing WARC files.
+ Use the `-name` option to specify the name of the archive (will correspond to the HBase table name).
+ Use the `-create` option to create a new table (and drop the existing table if a table with the same name exists already). Alternatively, use `-append` to add to an existing table.

Starting the browser:

```
$ setenv CLASSPATH_PREFIX "/etc/hbase/conf/"
$ sh target/appassembler/bin/WarcBrowser -port 9191 -server http://myhost:9191/
```

Navigate to `http://myhost:9191/` to browse the archive.


BulkLoading Ingestion
------------------

MapReduce is very nicely integrated with HBase, which means HBase table can be
used as source and sink for MapReduce jobs. We exploit this feature by making a MapReduce job to output data in HBase’s internal format which can be loaded fast in HBase table. 

BulkLoading of ARC files
------------------------

This can be done in three steps:

First, create an HBase table with pre-partitions.

```
$ create 'TableName', {NAME =>'content'},{NAME = > 'type'}, {SPLITS => ['first boundary','second boundary', ...]}
```
Second, prepare the data in HBase’s internal storage format (HFile) using A MapReduce job that uses FileOutputFormat
as an output format. The mapper class has to be implemented by the programmer, the map output key is the row key of the HBase table, and the value is a Put statement. The reducer class handled by HBase, HFileOutputFormat has a convenience function called configureIncrementalLoad(), which sets up Hadoop’s TotalOrderPartitioner class based on the table boundaries, which represent the number
of pre-assigned splits when the table was created.

```
$ hadoop jar target/warcbase-0.1.0-SNAPSHOT-fatjar.jar org.warcbase.cwi.bulkloading -in inputDir  -out outDir -table table-created-step1
```

Third, complete loading the HFiles into HBase table using HBase completebulkload tool.

```
$ hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles outDir_fromStep2 TableName
```


Extracting the Webgraph
-----------------------

First, use a MapReduce tool to extract all URLs from ARC data:

```
$ hadoop jar target/warcbase-0.1.0-SNAPSHOT-fatjar.jar \
  org.warcbase.analysis.demo.MapReduceArcDemo \
  -input inputDir -output outputDir
```

Next, build an FST for the URLs (using Lucene's FST package). The two relevant classes in Warcbase are:

+ `UriMappingBuilder`: takes a list of URLs as input and builds the FST mapping.
+ `UriMapping`: load the mapping file generated by Builder and provides an API for accessing the FST.

To build the Lucene FST :

```
$ sh target/appassembler/bin/UriMappingBuilder inputDirectory outputFile
```

This command will read all files under `inputDirectory` as input, build an FST, and write the data file to `outputFile`. The `UriMapping` class provides a simple command-line interface:


```
# Lookup by URL, fetches the integer id
$ sh target/appassembler/bin/UriMapping -data fst.dat -getId http://www.foo.com/

# Lookup by id, fetches the URL
$ sh target/appassembler/bin/UriMapping -data fst.dat -getUrl 42

# Fetches all URLs with the prefix
$ sh target/appassembler/bin/UriMapping -data fst.dat -getPrefix http://www.foo.com/
```

Then, we can use the FST mapping data to extract the webgraph and at the same time map URLs to unique integer ids. This is accomplished by a Hadoop program:

```
$ hadoop jar target/warcbase-0.1.0-SNAPSHOT-fatjar.jar org.warcbase.data.ExtractLinks 
  -input inputDir -output outputDir -uriMapping fstData -numReducers 1
```

Finally, instead of extracting links between individual URLs, we can extract the site-level webgraph by merging all URLs with common prefix into a "supernode". Link counts between supernodes represent the total number of links between their sub-URLs. In order to do this, following input files are required:

+ a prefix file providing URL prefixes for each supernode (comma-delimited: id, URL prefix);
+ an FST mapping file to map individual URLs to unique integer ids (from above);
+ a directory containing the original link graph (which can be extracted from the previous step).

Then run this program:

```
$ sh target/appassembler/bin/ExtractSiteLinks -prefixfile prefix.data 
  -fstfile fst.data -linkdir extract-links-data  -output sitelinks.data
```
