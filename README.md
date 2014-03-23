hdt-report-generator
====================

Generate report using Hadoop based on data in HDFS and metadata in RDBMS.

Configure the database connection using /hdt-report-generator/src/main/resources/dbconfig.properties.example

Usage: hadoop jar hdt-report-generator.jar DB_EXPORT_PATH INPUT_DIRECTORY OUTPUT_DIRECTORY -libjars LIBJARS
                DB_EXPORT_PATH - the directory in HDFS where the DB file will be exported in MapFile format
                INPUT_DIRECTORY - the directory in HDFS where the log files are
                OUTPUT_DIRECTORY - the directory in HDFS where the report will be placed
                LIBJARS - path to the DB connector dependency (example: mysql-connector-java.jar)
