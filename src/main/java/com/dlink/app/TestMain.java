package com.dlink.app;

import com.dlink.cdc.CDCBuilder;
import com.dlink.cdc.MysqlCDCBuilder;
import com.dlink.model.FlinkCDCConfig;
import com.dlink.model.Schema;
import com.dlink.sql.SQLSinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @PACKAGE_NAME: com.dlink.app
 * @NAME: TestMain
 * @USER: Rison
 * @DATE: 2022/10/8 9:21
 * @PROJECT_NAME: dlink-database-cdc
 **/
public class TestMain {
    public static void main(String[] args) throws Exception {
//        private String type;
//        private String hostname;
//        private Integer port;
//        private String username;
//        private String password;
//        private Integer checkpoint;
//        private Integer parallelism;
//        private String database;
//        private String schema;
//        private String table;
//        private List<String> schemaTableNameList;
//        private String startupMode;
//        private Map<String, String> debezium;
//        private Map<String, String> source;
//        private Map<String, String> jdbc;
//        private Map<String, String> sink;
//        private List<Schema> schemaList;
//        private String schemaFieldName;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        final FlinkCDCConfig flinkCDCConfig = new FlinkCDCConfig();
        flinkCDCConfig.setType("mysql-cdc");
        flinkCDCConfig.setHostname("tbds-192-168-0-37");
        flinkCDCConfig.setPort(3306);
        flinkCDCConfig.setUsername("root");
        flinkCDCConfig.setPassword("metadata@Tbds.com");
        flinkCDCConfig.setCheckpoint(5000);
        flinkCDCConfig.setParallelism(1);
        flinkCDCConfig.setDatabase("rison_db");
        flinkCDCConfig.setSchema("rison_db");
        flinkCDCConfig.setTable("student");
        flinkCDCConfig.setSchemaTableNameList(Collections.<String>singletonList("rison_db.student"));
        flinkCDCConfig.setStartupMode("initial");
        flinkCDCConfig.setDebezium(new HashMap<>());
        flinkCDCConfig.setSource(new HashMap<>());
        flinkCDCConfig.setJdbc(new HashMap<>());
        flinkCDCConfig.setSink(new HashMap<>());
//        flinkCDCConfig.setSchemaList(new List<>());
        flinkCDCConfig.setSchemaFieldName("db");

        final CDCBuilder cdcBuilder = new MysqlCDCBuilder().create(flinkCDCConfig);
        final DataStreamSource<String> streamSource = cdcBuilder.build(env);

        streamSource.print();

       new SQLSinkBuilder().create(flinkCDCConfig).build(env, streamSource);


        env.execute("testMain");

    }
}
/*
/usr/hdp/2.2.0.0-2041/flink/bin/flink run \
-t yarn-per-job \
-p 1 \
-c com.dlink.app.TestMain \
/root/flink-dir/flink-cdc.jar
 */