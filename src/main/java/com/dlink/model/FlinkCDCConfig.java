package com.dlink.model;

import java.util.List;
import java.util.Map;

/**
 * @PACKAGE_NAME: com.dlink.model
 * @NAME: FlinkCDCConfig
 * @USER: Rison
 * @DATE: 2022/10/5 14:11
 * @PROJECT_NAME: dlink-database-cdc
 **/
public class FlinkCDCConfig {
    private String type;
    private String hostname;
    private Integer port;
    private String username;
    private String password;
    private Integer checkpoint;
    private Integer parallelism;
    private String database;
    private String schema;
    private String table;
    private List<String> schemaTableNameList;
    private String startupMode;
    private Map<String, String> debezium;
    private Map<String, String> source;
    private Map<String, String> jdbc;
    private Map<String, String> sink;
    private List<Schema> schemaList;
    private String schemaFieldName;
}
