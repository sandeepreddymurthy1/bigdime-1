/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.constants;

/**
 * Properties that are used in ActionEvent headers by different handlers.
 * 
 * @author Neeraj Jain
 *
 */
public final class ActionEventHeaderConstants {
	private static final ActionEventHeaderConstants instance = new ActionEventHeaderConstants();

	private ActionEventHeaderConstants() {
	}

	public static ActionEventHeaderConstants getInstance() {
		return instance;
	}

	public static final String HIVE_PARTITION_NAMES = "hive_partition_names";
	public static final String HIVE_PARTITION_VALUES = "hive_partition_values";
	public static final String HDFS_FILE_NAME = "hdfsFileName";
	public static final String HDFS_PATH = "hdfsPath";
	public static final String COMPLETE_HDFS_PATH = "completeHdfsPath";
	public static final String HOST_NAMES = "hostNames";
	public static final String PORT = "port";
	public static final String RECORD_COUNT = "recordCount";

	// Complete path of the file being read, with the filename
	public static final String SOURCE_FILE_PATH = "sourceFilePath";// same as
																	// absoluteFilePath
	public static final String SOURCE_FILE_LOCATION = "sourceFileLocation";
	public static final String SOURCE_RECORD_COUNT = "recordCount";

	public static final String ENTITY_NAME = "entityName";
	public static final String INPUT_DESCRIPTOR = "inputDescriptor";

	public static final String SOURCE_FILE_NAME = "sourceFileName";

//	public static final String ABSOLUTE_FILE_PATH = "absoluteFilePath"; // absolute
																		// file
																		// path
																		// with
																		// name
	public static final String PRESERVE_BASE_PATH = "preserveBasePath";
	public static final String PRESERVE_RELATIVE_PATH = "preserveRelativePath";
	public static final String BASE_PATH = "basePath";
	public static final String RELATIVE_PATH = "relativePath";
	public static final String SOURCE_FILE_TOTAL_SIZE = "sourceFileTotalSize";
	public static final String SOURCE_FILE_TOTAL_READ = "sourceFileTotalRead";
	public static final String USER_NAME = "user.name";
	public static final String DB_NAME = "databaseName";
	
	public static final String HDFS_FILE_LOCATION = "fileLocation";
	public static final String HDFS_BAST_PATH = "basePath";
	
	public static final String HIVE_TABLE_LOCATION = "hiveTableLocation";

	public static final String SKIP_DATABASE_CREATION = "skipDatabaseCreation";
	public static final String SKIP_TABLE_CREATION = "skipTableCreation";
	public static final String FIELDS_TERMINATED_BY = "fieldsTerminatedBy";
	public static final String LINES_TERMINATED_BY = "linesTerminatedBy";
	public static final String HIVE_PARTITION_LOCATION = "hivePartitionLocation";
	public static final String CLEANUP_REQUIRED = "cleanupRequired";
	public static final String READ_COMPLETE = "readComplete";	
	
	public static final String DATE = "dt";
	public static final String SNAPSHOT = "SNAPSHOT";
	public static final String TARGET_ENTITY_NAME = "targetEntityName";
	public static final String HIVE_NON_PARTITION_NAMES = "hive_nonpartition_names";
	public static final String HIVE_PARTITION_REQUIRED = "hivePartitionRequired";
	
	public static final String SCHEMA_TYPE_HIVE = "HIVE";
	public static final String HIVE_HOST_NAME = "hiveHostName";
	public static final String HIVE_PORT = "hivePort";
	public static final String HIVE_DB_NAME = "hiveDBName";
	public static final String HIVE_TABLE_NAME = "hiveTableName";
	public static final String VALIDATION_READY = "validationReady";
	public static final String HIVE_METASTORE_URI = "hive.metastore.uris";
	
}
