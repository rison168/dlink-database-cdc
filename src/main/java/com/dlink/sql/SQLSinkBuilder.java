package com.dlink.sql;

import com.dlink.cdc.AbstractSinkBuilder;
import com.dlink.cdc.CDCBuilder;
import com.dlink.cdc.SinkBuilder;
import com.dlink.executor.CustomTableEnvironment;
import com.dlink.model.FlinkCDCConfig;
import com.dlink.model.Table;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import java.io.Serializable;
import java.time.ZoneId;
import java.util.List;

/**
 * @PACKAGE_NAME: com.dlink.sql
 * @NAME: SQLSinkBuilder
 * @USER: Rison
 * @DATE: 2022/10/5 19:59
 * @PROJECT_NAME: dlink-database-cdc
 **/
public class SQLSinkBuilder extends AbstractSinkBuilder implements SinkBuilder, Serializable {
    private static final long serialVersionUID = 5529171111869024688L;
    private static final String KEY_WORD = "sql";
    private ZoneId sinkTimeZone = ZoneId.of("UTC");

    public SQLSinkBuilder(){}

    private SQLSinkBuilder(FlinkCDCConfig config){
        super(config);
    }


    @Override
    public void addSink(StreamExecutionEnvironment env, DataStream<RowData> rowDataDataStream, Table table, List<String> columnNameList, List<LogicalType> columnTypeList) {

    }

    @Override
    public String getHandle() {
        return KEY_WORD;
    }

    @Override
    public SinkBuilder create(FlinkCDCConfig config) {
        return  new SQLSinkBuilder(config);
    }

    @Override
    public DataStreamSource build(CDCBuilder cdcBuilder, StreamExecutionEnvironment env, CustomTableEnvironment customTableEnvironment, DataStreamSource<String> dataStreamSource) {
        return super.build(cdcBuilder, env, customTableEnvironment, dataStreamSource);
    }
}
