package com.dlink.cdc;

import com.dlink.model.FlinkCDCConfig;
import executor.CustomTableEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @PACKAGE_NAME: com.dlink.cdc
 * @NAME: SinkBuilder
 * @USER: Rison
 * @DATE: 2022/10/5 14:02
 * @PROJECT_NAME: dlink-database-cdc
 **/
public interface SinkBuilder {
    String getHandle();
    SinkBuilder create(FlinkCDCConfig config);
    DataStreamSource build(CDCBuilder cdcBuilder, StreamExecutionEnvironment env, CustomTableEnvironment customTableEnvironment, DataStreamSource<String> dataStreamSource);
}
