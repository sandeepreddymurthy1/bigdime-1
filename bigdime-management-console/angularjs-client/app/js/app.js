/**
 * Copyright (C) 2015 Stubhub.
 */
/**
 * @author Sandeep Reddy,Murthy
 * @ngdoc function
 * @description # Provides application constants and routes for the application
 */
'use strict';

/**
 * @ngdoc overview
 * @name jsonerApp
 * @description
 * # jsonerApp
 *
 * Main module of the application.
 */
angular
.module('jsonerApp', ['ui.router'])
.constant("AdaptorConstants",[{
	"environment":"Dev",
	"adaptors":[{name:'Kafka',
        handlerlist:['avro-record-handler','hive-json-mapper'],
        handlerclasslist:['io.bigdime.handler.kafka.KafkaReaderHandler','io.bigdime.handler.avro.AvroJsonMapperHandler','io.bigdime.handler.hive.JsonHiveSchemaMapperHandler'],
        sinkhandlerlist:['memory-channel-reader','hdfs-sink-adaptor','data-validation-handler','hive-meta-handler'],
        sinkhandlerclasslist:['io.bigdime.core.handler.MemoryChannelInputHandler','io.bigdime.handler.webhdfs.WebHDFSWriterHandler','io.bigdime.core.handler.DataValidationHandler','io.bigdime.handler.hive.HiveMetaDataHandler'],
        channellist:['Memory channel'],
        nondefaults:['description','properties.offset-data-dir','properties.brokers','properties.messageSize','properties.buffer-size','properties.schemaFileName'],
        defaults:{ "type": "streaming", "source": { "sourcetype": "kafka", "datahandlers": [ { "name": "kafka-data-reader", "handlerclass": "io.bigdime.handler.kafka.KafkaReaderHandler", "properties": { "brokers": "${kafka_brokers}" } } ] }, "sink": { "channeldesc": [ "Memory Channel" ], "datahandlers": [ { "name": "memory-channel-reader", "handlerclass": "io.bigdime.core.handler.MemoryChannelInputHandler", "properties": {} } ] }, "channel": [ { "name": "Memory Channel", "channelclass": "io.bigdime.core.handler.MemoryChannelInputHandler", "properties": {} } ] },
        nondefault:{ "name": "", "cronexpression": "", "autostart": "", "namespace": "", "description": "", "source": { "name": "", "description": "", "datahandlers": [ { "description": "", "properties": { "offsetdatadir": "", "messagesize": "" } } ] }, "sink": { "name": "", "description": "", "channeldesc": [ "Memory Channel" ], "datahandlers": [ { "description": "", "properties": { "batchsize": "" } } ] }, "channel": [ { "description": "", "properties": { "batchsize": "", "printstats": "" } } ] },
        mandatoryfields:['name','type','cron-expression','auto-start','namespace','sourcename','handler-class','brokers','offset-data-dir','messageSize','buffer-size','schemaFileName','entityName','hostNames','port','hdfsPath','hdfsUser','hdfsOverwrite','hdfsPermissions','channel-class','print-stats','print-stats-duration-in-seconds'],
        nonessentialfields:['schemaFileName','description','hdfs-path-lower-upper-case','hdfsFileNamePrefix','hdfsFileNameExtension'],
        loadhtmlfields:{"adaptor":[{'name':'name'},{'name':'type'},{'name':'cron-expression'},{'name':'auto-start'},{'name':'namespace'},{'name':'description'}],"source":[],"sink":[],"channel":[]}
       },{name:'File',
          handlerlist:['zip-file-data-reader','partition-name-parser','file-data-reader'],
          handlerclasslist:['io.bigdime.handler.file.ZipFileInputStreamHandler','io.bigdime.handler.file.PartitionParserHandler','io.bigdime.handler.file.FileInputStreamHandler'],
          sinkhandlerlist:['hdfs-sink-adaptor','data-validation-handler','hive-meta-handler'],
          sinkhandlerclasslist:['io.bigdime.core.handler.MemoryChannelInputHandler','io.bigdime.handler.webhdfs.WebHDFSWriterHandler','io.bigdime.core.handler.DataValidationHandler','io.bigdime.handler.hive.HiveMetaDataHandler'],
          channellist:['Memory channel'],
	       nondefaults:['description','handlerclass','properties.base-path','properties.buffer-size','properties.preserve-base-path','properties.preserve-relative-path','properties.partition-names','properties.regex'],
	       mandatoryfields:['name','type','cron-expression','auto-start','namespace','handler-class','buffer-size','base-path','preserve-base-path','preserve-relative-path','partition-names','regex','batchSize','sinkName','hostNames','port','hdfsPath','hdfsUser','hdfsOverwrite','hdfsPermissions','validation-type','hive','channel-class','print-stats','print-stats-duration-in-seconds'],
	       nonessentialfields:['description','date-partition-name','date-partition-input-format','date-partition-output-format','hdfsFileNamePrefix','hdfsFileNameExtension','batchSize','date-partition-name','date-partition-input-format','date-partition-output-format','partition-names-output-order','hdfs-path-lower-upper-case','header-name','sourceDescription'],
	       loadhtmlfields:{"adaptor":[{'name':'name'},{'name':'type'},{'name':'cron-expression'},{'name':'auto-start'},{'name':'namespace'},{'name':'description'}],"source":[],"sink":[],"channel":[]}
       },{name:'Sql',
          handlerlist:['jdbc-data-reader','jdbc-data-cleanos','jdbc-database-schema-reader'],
          handlerclasslist:['io.bigdime.handler.jdbc.JDBCReaderHandler','io.bigdime.handler.jdbc.DataCleansingHandler'],
          sinkhandlerlist:['memory-channel-reader','hdfs-sink-adaptor'],
          sinkhandlerclasslist:['io.bigdime.core.handler.MemoryChannelInputHandler','io.bigdime.handler.webhdfs.WebHDFSWriterHandler'],	               
	       channellist:['Memory channel'],
	       nondefaults:['description','handlerclass','properties.basepath','properties.buffersize','properties.preservebasepath','properties.preserverelativepath'],
	       mandatoryfields:['name','type','cron-expression','auto-start','namespace','sourcename','handler-class','buffer-size','base-path','preserve-base-path','preserve-relative-path','sinkname','batchSize','hostNames','port','hdfsFileNamePrefix','hdfsFileNameExtension','hdfsPath','hdfsUser','hdfsOverwrite','hdfsPermissions'],
	       nonessentialfields:['description']
       }
	    
	            ]
	
},{
	"environment":"Qa",
	"adaptors":[{name:'Kafka',
        handlerlist:['avro-record-handler','hive-json-mapper'],
        handlerclasslist:['io.bigdime.handler.kafka.KafkaReaderHandler','io.bigdime.handler.avro.AvroJsonMapperHandler','io.bigdime.handler.hive.JsonHiveSchemaMapperHandler'],
        sinkhandlerlist:['memory-channel-reader','hdfs-sink-adaptor','data-validation-handler','hive-meta-handler'],
        sinkhandlerclasslist:['io.bigdime.core.handler.MemoryChannelInputHandler','io.bigdime.handler.webhdfs.WebHDFSWriterHandler','io.bigdime.core.handler.DataValidationHandler','io.bigdime.handler.hive.HiveMetaDataHandler'],
        channellist:['Memory channel'],
        nondefaults:['description','properties.offset-data-dir','properties.brokers','properties.messageSize','properties.buffer-size','properties.schemaFileName'],
        defaults:{ "type": "streaming", "source": { "sourcetype": "kafka", "datahandlers": [ { "name": "kafka-data-reader", "handlerclass": "io.bigdime.handler.kafka.KafkaReaderHandler", "properties": { "brokers": "${kafka_brokers}" } } ] }, "sink": { "channeldesc": [ "Memory Channel" ], "datahandlers": [ { "name": "memory-channel-reader", "handlerclass": "io.bigdime.core.handler.MemoryChannelInputHandler", "properties": {} } ] }, "channel": [ { "name": "Memory Channel", "channelclass": "io.bigdime.core.handler.MemoryChannelInputHandler", "properties": {} } ] },
        nondefault:{ "name": "", "cronexpression": "", "autostart": "", "namespace": "", "description": "", "source": { "name": "", "description": "", "datahandlers": [ { "description": "", "properties": { "offsetdatadir": "", "messagesize": "" } } ] }, "sink": { "name": "", "description": "", "channeldesc": [ "Memory Channel" ], "datahandlers": [ { "description": "", "properties": { "batchsize": "" } } ] }, "channel": [ { "description": "", "properties": { "batchsize": "", "printstats": "" } } ] },
        mandatoryfields:['name','type','cron-expression','auto-start','namespace','sourcename','handler-class','brokers','offset-data-dir','messageSize','buffer-size','schemaFileName','entityName','hostNames','port','hdfsPath','hdfsUser','hdfsOverwrite','hdfsPermissions','channel-class','print-stats','print-stats-duration-in-seconds'],
        nonessentialfields:['schemaFileName','description','hdfs-path-lower-upper-case','hdfsFileNamePrefix','hdfsFileNameExtension'],
        loadhtmlfields:{"adaptor":[{'name':'name'},{'name':'type'},{'name':'cron-expression'},{'name':'auto-start'},{'name':'namespace'},{'name':'description'}],"source":[],"sink":[],"channel":[]}
       },{name:'File',
          handlerlist:['zip-file-data-reader','partition-name-parser','file-data-reader'],
          handlerclasslist:['io.bigdime.handler.file.ZipFileInputStreamHandler','io.bigdime.handler.file.PartitionParserHandler','io.bigdime.handler.file.FileInputStreamHandler'],
          sinkhandlerlist:['hdfs-sink-adaptor','data-validation-handler','hive-meta-handler'],
          sinkhandlerclasslist:['io.bigdime.core.handler.MemoryChannelInputHandler','io.bigdime.handler.webhdfs.WebHDFSWriterHandler','io.bigdime.core.handler.DataValidationHandler','io.bigdime.handler.hive.HiveMetaDataHandler'],
          channellist:['Memory channel'],
	       nondefaults:['description','handlerclass','properties.base-path','properties.buffer-size','properties.preserve-base-path','properties.preserve-relative-path','properties.partition-names','properties.regex'],
	       mandatoryfields:['name','type','cron-expression','auto-start','namespace','handler-class','buffer-size','base-path','preserve-base-path','preserve-relative-path','partition-names','regex','batchSize','sinkName','hostNames','port','hdfsPath','hdfsUser','hdfsOverwrite','hdfsPermissions','validation-type','hive','channel-class','print-stats','print-stats-duration-in-seconds'],
	       nonessentialfields:['description','date-partition-name','date-partition-input-format','date-partition-output-format','hdfsFileNamePrefix','hdfsFileNameExtension','batchSize','date-partition-name','date-partition-input-format','date-partition-output-format','partition-names-output-order','hdfs-path-lower-upper-case','header-name','sourceDescription'],
	       loadhtmlfields:{"adaptor":[{'name':'name'},{'name':'type'},{'name':'cron-expression'},{'name':'auto-start'},{'name':'namespace'},{'name':'description'}],"source":[],"sink":[],"channel":[]}
       },{name:'Sql',
          handlerlist:['jdbc-data-reader','jdbc-data-cleanos','jdbc-database-schema-reader'],
          handlerclasslist:['io.bigdime.handler.jdbc.JDBCReaderHandler','io.bigdime.handler.jdbc.DataCleansingHandler'],
          sinkhandlerlist:['memory-channel-reader','hdfs-sink-adaptor'],
          sinkhandlerclasslist:['io.bigdime.core.handler.MemoryChannelInputHandler','io.bigdime.handler.webhdfs.WebHDFSWriterHandler'],	               
	       channellist:['Memory channel'],
	       nondefaults:['description','handlerclass','properties.basepath','properties.buffersize','properties.preservebasepath','properties.preserverelativepath'],
	       mandatoryfields:['name','type','cron-expression','auto-start','namespace','sourcename','handler-class','buffer-size','base-path','preserve-base-path','preserve-relative-path','sinkname','batchSize','hostNames','port','hdfsFileNamePrefix','hdfsFileNameExtension','hdfsPath','hdfsUser','hdfsOverwrite','hdfsPermissions'],
	       nonessentialfields:['description']
       }
	            ]
	
},{
	"environment":"Prod",
	"adaptors":[{name:'Kafka',
        handlerlist:['avro-record-handler','hive-json-mapper'],
        handlerclasslist:['io.bigdime.handler.kafka.KafkaReaderHandler','io.bigdime.handler.avro.AvroJsonMapperHandler','io.bigdime.handler.hive.JsonHiveSchemaMapperHandler'],
        sinkhandlerlist:['memory-channel-reader','hdfs-sink-adaptor','data-validation-handler','hive-meta-handler'],
        sinkhandlerclasslist:['io.bigdime.core.handler.MemoryChannelInputHandler','io.bigdime.handler.webhdfs.WebHDFSWriterHandler','io.bigdime.core.handler.DataValidationHandler','io.bigdime.handler.hive.HiveMetaDataHandler'],
        channellist:['Memory channel'],
        nondefaults:['description','properties.offset-data-dir','properties.brokers','properties.messageSize','properties.buffer-size','properties.schemaFileName'],
        defaults:{ "type": "streaming", "source": { "sourcetype": "kafka", "datahandlers": [ { "name": "kafka-data-reader", "handlerclass": "io.bigdime.handler.kafka.KafkaReaderHandler", "properties": { "brokers": "${kafka_brokers}" } } ] }, "sink": { "channeldesc": [ "Memory Channel" ], "datahandlers": [ { "name": "memory-channel-reader", "handlerclass": "io.bigdime.core.handler.MemoryChannelInputHandler", "properties": {} } ] }, "channel": [ { "name": "Memory Channel", "channelclass": "io.bigdime.core.handler.MemoryChannelInputHandler", "properties": {} } ] },
        nondefault:{ "name": "", "cronexpression": "", "autostart": "", "namespace": "", "description": "", "source": { "name": "", "description": "", "datahandlers": [ { "description": "", "properties": { "offsetdatadir": "", "messagesize": "" } } ] }, "sink": { "name": "", "description": "", "channeldesc": [ "Memory Channel" ], "datahandlers": [ { "description": "", "properties": { "batchsize": "" } } ] }, "channel": [ { "description": "", "properties": { "batchsize": "", "printstats": "" } } ] },
        mandatoryfields:['name','type','cron-expression','auto-start','namespace','sourcename','handler-class','brokers','offset-data-dir','messageSize','buffer-size','schemaFileName','entityName','hostNames','port','hdfsPath','hdfsUser','hdfsOverwrite','hdfsPermissions','channel-class','print-stats','print-stats-duration-in-seconds'],
        nonessentialfields:['schemaFileName','description','hdfs-path-lower-upper-case','hdfsFileNamePrefix','hdfsFileNameExtension'],
        loadhtmlfields:{"adaptor":[{'name':'name'},{'name':'type'},{'name':'cron-expression'},{'name':'auto-start'},{'name':'namespace'},{'name':'description'}],"source":[],"sink":[],"channel":[]}
       },{name:'File',
          handlerlist:['zip-file-data-reader','partition-name-parser','file-data-reader'],
          handlerclasslist:['io.bigdime.handler.file.ZipFileInputStreamHandler','io.bigdime.handler.file.PartitionParserHandler','io.bigdime.handler.file.FileInputStreamHandler'],
          sinkhandlerlist:['hdfs-sink-adaptor','data-validation-handler','hive-meta-handler'],
          sinkhandlerclasslist:['io.bigdime.core.handler.MemoryChannelInputHandler','io.bigdime.handler.webhdfs.WebHDFSWriterHandler','io.bigdime.core.handler.DataValidationHandler','io.bigdime.handler.hive.HiveMetaDataHandler'],
          channellist:['Memory channel'],
	       nondefaults:['description','handlerclass','properties.base-path','properties.buffer-size','properties.preserve-base-path','properties.preserve-relative-path','properties.partition-names','properties.regex'],
	       mandatoryfields:['name','type','cron-expression','auto-start','namespace','handler-class','buffer-size','base-path','preserve-base-path','preserve-relative-path','partition-names','regex','batchSize','sinkName','hostNames','port','hdfsPath','hdfsUser','hdfsOverwrite','hdfsPermissions','validation-type','hive','channel-class','print-stats','print-stats-duration-in-seconds'],
	       nonessentialfields:['description','date-partition-name','date-partition-input-format','date-partition-output-format','hdfsFileNamePrefix','hdfsFileNameExtension','batchSize','date-partition-name','date-partition-input-format','date-partition-output-format','partition-names-output-order','hdfs-path-lower-upper-case','header-name','sourceDescription'],
	       loadhtmlfields:{"adaptor":[{'name':'name'},{'name':'type'},{'name':'cron-expression'},{'name':'auto-start'},{'name':'namespace'},{'name':'description'}],"source":[],"sink":[],"channel":[]}
       },{name:'Sql',
          handlerlist:['jdbc-data-reader','jdbc-data-cleanos','jdbc-database-schema-reader'],
          handlerclasslist:['io.bigdime.handler.jdbc.JDBCReaderHandler','io.bigdime.handler.jdbc.DataCleansingHandler'],
          sinkhandlerlist:['memory-channel-reader','hdfs-sink-adaptor'],
          sinkhandlerclasslist:['io.bigdime.core.handler.MemoryChannelInputHandler','io.bigdime.handler.webhdfs.WebHDFSWriterHandler'],	               
	       channellist:['Memory channel'],
	       nondefaults:['description','handlerclass','properties.basepath','properties.buffersize','properties.preservebasepath','properties.preserverelativepath'],
	       mandatoryfields:['name','type','cron-expression','auto-start','namespace','sourcename','handler-class','buffer-size','base-path','preserve-base-path','preserve-relative-path','sinkname','batchSize','hostNames','port','hdfsFileNamePrefix','hdfsFileNameExtension','hdfsPath','hdfsUser','hdfsOverwrite','hdfsPermissions'],
	       nonessentialfields:['description']
       }
	    
	            ]
	
}])
.config(function ($stateProvider, $urlRouterProvider) {
	$urlRouterProvider.otherwise('/indexpage');
	
	$stateProvider
	 .state('indexpage', {
         url: '/indexpage',
         templateUrl: 'views/index/login.html',
         controller: 'LogincontrollerCtrl'
     })
    .state('homepage', {
         url: '/homepage',
         views:{
        	 '':{templateUrl:'views/home/homepage.html',
        		 controller:'HomecontrollerCtrl'
        		 },
        	 'applicationtree@homepage':{
        		 templateUrl:'views/home/hometree.html',
        		 controller:'HomecontrollerCtrl'
        	 },
        	 'applicationheader@homepage':{
        		 templateUrl:'views/home/applicationheader.html',
        		 controller:'HomecontrollerCtrl'
        	 }
    
         }
     })
     .state('homepage.search',{
    	 url: '/search',
         templateUrl: 'views/search/homesearch.html',
         controller: 'SearchcontrollerCtrl'
     })
     .state('homepage.jsonhome',{
    	 url: '/jsonhome',
         templateUrl: 'views/adaptorsselection/selectortable.html',
         controller: 'JsoncontrollerCtrl'
     })
	 .state('homepage.sourcehandlers', {
         url: '/sourcehandlers',
         templateUrl: 'views/sourcehandlerselection/sourcehandlers.html',
         controller: 'SourcehandlercontrollerCtrl'
     })
      .state('homepage.sinkhandlers', {
         url: '/sinkhandlers',
         templateUrl: 'views/sinkhandlerselection/sinkhandlers.html',
         controller: 'SinkhandlercontrollerCtrl'
     })
     .state('homepage.channelselector', {
         url: '/channelselector',
         templateUrl: 'views/channelselection/channelselector.html',
         controller: 'ChannelcontrollerCtrl'
     })
     .state('homepage.sourcechannelsinkmapper', {
         url: '/sourcechannelsinkmapper',
         templateUrl: 'views/sourcechannelsinkselection/sourcechannelsinkmapper.html',
         controller: 'MappercontrollerCtrl'
     })
     .state('homepage.viewjson', {
         url: '/viewjson',
         templateUrl: 'views/sourcechannelsinkselection/viewjson.html',
         controller: 'FinalJsonCtrl'
     });
});