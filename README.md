### What is BigDime  : 
BigDime is code name for Data Ingestion Eco System Project. it stands for Big Data Ingestion Made Easy. it consists of following things.

Core Data Ingestion Framework - that leverages apache flume 

Handler Orchestration framework  - that allows to building different handlers to ingest different data sources and formats.

Data Adaptors - Configuration  driven group of handlers for ingesting any particular data source. ( an example :  click stream,preferences..etc)

Metastore  - Core framework for metadata management, for immediate querablity of ingested data with appropriate partitions.

Monitoring - Framework for monitoring  system failures and alerts on them.

Management Console - console to providing visibility to available data and running jobs/ applications status.

Getting Started
=======
To get started, you need to obtain a bigdime distribution and execute the install steps: There are two ways to obtain a bigdime distribution: [Download It!](#download) or [Build It!](#build).
1. Obtain a distribtion
-
<a name="download">
## Download
</a>

1. The release can be downloaded from [here](https://oss.sonatype.org/content/groups/public/io/bigdime/bigdime-dist/).
2. Download the bigdime-dist-${version}-bin.tar.gz file.

-OR-
<a name="build">
## Build
</a>

1. Clone the BigDime repository to, say /opt/bigdime/repo.
2. From the /opt/bigdime/repo path, run "mvn clean package". This may take a couple of minutes to complete.
3. Verify that bigdime-dist-${version}-bin.tar.gz exists in /opt/bigdime/repo/dist/target/.

2. Install
-
4. Download the release or build it yourself and copy the built artifact to a path, say /path/bigdime.
5. Navigate to /path/bigdime and untar the file using following command:
	4. tar xvf /path/bigdime/bigdime-dist-${version}-bin.tar.gz
	5. Above command will extract following files inside bigdime-dist-${version} directory:
		6. bigdime-adaptor-${version}.jar
		6. config/application.properties.template
		7. config/log4j.properties.template
		7. config/META-INF/adaptor.json.template.file
		8. config/META-INF/adaptor.json.template.kafka
		9. scripts/MY_SQL_ARIM_DDL_SCRIPTS.sql
		10. scripts/MY_SQL_MDM_DDL_SCRIPTS.sql
		8. logs/
9. Execute following command to create mysql tables
	13. Create bigdime_metadata in MySql.
	14. Create tables for metadata management module:
		15. mysql -u {username} -p{password} bigdime_metadata < MY\_SQL\_MDM\_DDL\_SCRIPTS.sql
	14. Create tables for adaptor runtime information management module:
		15. mysql -u {username} -p{password} bigdime_metadata < MY\_SQL\_ARIM\_DDL\_SCRIPTS.sql
15. Create adaptor.json in /path/bigdime/bigdime-dist-${version}/config/META-INF/ to complete the adaptor configuration, use one of the adaptor.json.template files as a starting point.
10. Create application.properties in /path/bigdime/bigdime-dist-${version}/config/, use application.properties.template file as a starting point. Update the URLs, paths, usernames, passwords accordingly.
11. Create log4j.properties in /opt/bigdime/bigdime-dist-${version}/config/, use log4j.properties.template file as a starting point.
3. Run
-
Run the following command from /path/bigdime/bigdime-dist-${version}:
java -jar -Dloader.path=./config/ -Denv.properties=application.properties bigdime-adaptor-${version}.jar

