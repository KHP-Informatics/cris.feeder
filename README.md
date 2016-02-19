# cris.feeder
Feed Gate GCP with CRIS data streaming from SQL database.
This feeder implements the following two Gate GCP's interfaces.

1. DocumentEnumerator: for listing the document IDs.
2. InputHandler: for retrieving the full doc from the database.

To use the feeder, these two implementations are to be setup in the GCP batch file (see `conf/gcp-batch.xml`).

###configuration
The configuration file is named config.properties, which should be loadable at runtime, e.g., in the classpath. It contains the settings of the database driver, JDBC URL and necessary table schema information. It also contains configurations of the total number of documents to be processed and the number of documents to be cached for each SQL query. Please refer the configuration file for detail.

###lib
One cutomised library used is hwudbutil-1.2.jar, which was written in a research project many years ago for helping junior JAVA developers dealing with DB operations. It supports connection pooling and, most importantly, frees developers from the headache of connection management. 
