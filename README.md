# cris.feeder
Part of a scalable execution of Sheffied University's bio-yodie (natural language processing) pipeline over the whole set of CRIS database. This code is part of Deliverable 4.4 of [EU KConnect project](http://kconnect.eu/).

- (updates - 27 Nov 2016) 
As part of the KConnect project dissemination activities, bio-yodie is now being used in Kings College Hospital for annotating their 10m EHR documents. To support this activity, Elastic Search based input/out handlers have been implemented.

## input handler
Feed [Gate GCP](https://gate.ac.uk/gcp/doc/gcp-guide.pdf) with CRIS data streaming from SQL database.
This feeder implements the following two Gate GCP's interfaces.

1. DocumentEnumerator: for listing the document IDs.
2. InputHandler: for retrieving the full doc from the database.

## output handler
An output handler is added to save the bio-yodie pipeline annotations as a bunch of JSON files, which can be further indexed or processed.

##configuration
The configuration file is named config.properties, which should be loadable at runtime, e.g., in the classpath. It contains the settings of the database driver, JDBC URL and necessary table schema information. It also contains configurations of the total number of documents to be processed and the number of documents to be cached for each SQL query. Please refer the configuration file for detail.

## usage
To use the feeder, these two handlers are to be setup in the GCP batch file (see `conf/gcp-batch.xml`).

The document enumerator setting in GCP configuration.
```xml
<documents>
  <documentEnumerator class="kcl.iop.brc.core.kconnect.crisfeeder.CRISDocEnumerator"/>
 </documents>
```

The input handler setting:
```xml
<input
	encoding =  "UTF-8"
	class	=  "kcl.iop.brc.core.kconnect.crisfeeder.CRISDocInputHandler"
	driver   =  "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  />
```

(Optional) if you want to store the output of bio-yodie annotations as JSON files for future use, you can follow these settings.

- put the following into GCP configuration file:
```xml
<output 
	class="kcl.iop.brc.core.kconnect.outputhandler.YodieOutputHandler"/>
```
- put the following in the config.properties file. Set the value of `outputFilePrefix` as the prefix of JSON file names including the full path, e.g., `/opt/bio-yodie-run/output/ann`. The tool will create JSON files by appending thread IDs. This means each thread will use a single JSON file as the output.

```
#output handler settings
annotationOutputSettings=[{"annotationSet":"Shef","annotationType":["Mention"]}]
outputFilePrefix=
```

### lib
One cutomised library used is hwudbutil-1.2.jar, which was written in a research project many years ago for helping junior JAVA developers dealing with DB operations. It supports connection pooling and, most importantly, frees developers from the headache of connection management. 
