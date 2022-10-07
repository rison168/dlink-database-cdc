package com.dlink.sql;

import com.dlink.cdc.AbstractSinkBuilder;
import com.dlink.cdc.CDCBuilder;
import com.dlink.cdc.SinkBuilder;
import com.dlink.executor.CustomTableEnvironment;
import com.dlink.model.FlinkCDCConfig;
import com.dlink.model.Table;
import com.dlink.utils.FlinkBaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
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

    public SQLSinkBuilder() {
    }

    private SQLSinkBuilder(FlinkCDCConfig config) {
        super(config);
    }


    @Override
    public void addSink(StreamExecutionEnvironment env, DataStream<RowData> rowDataDataStream, Table table, List<String> columnNameList, List<LogicalType> columnTypeList) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        final StreamTableEnvironment tblEnv = StreamTableEnvironmentImpl.create(env, settings, TableConfig.getDefault());
        final String sinkSchemaName = getSinkSchemaName(table);
        final String sinkTableName = getSinkTableName(table);
        final String pkList = StringUtils.join(getPKList(table), ".");
        final String viewName = "view_" + table.getSchemaTableNameWithUnderline();
        tblEnv.createTemporaryView(viewName, rowDataDataStream, StringUtils.join(columnNameList, ","));
        logger.info("Create " + viewName + " tableView successful!");
        String flinkDDL = FlinkBaseUtil.getFlinkDDL(table, sinkTableName, config, sinkSchemaName, sinkTableName, pkList);
        tblEnv.executeSql(flinkDDL);
        logger.info("Create " + sinkTableName + " FlinkSQL DDL successful!");
        String cdcSqlInsert = FlinkBaseUtil.getCDCSqlInsert(table, sinkTableName, viewName, config);
        tblEnv.executeSql(cdcSqlInsert);
        logger.info(cdcSqlInsert);
        logger.info("Create " + sinkTableName + " FlinkSQL insert into successful!");
    }

    @Override
    public String getHandle() {
        return KEY_WORD;
    }

    @Override
    public SinkBuilder create(FlinkCDCConfig config) {
        return new SQLSinkBuilder(config);
    }

    @Override
    public DataStreamSource build(CDCBuilder cdcBuilder, StreamExecutionEnvironment env, CustomTableEnvironment customTableEnvironment, DataStreamSource<String> dataStreamSource) {
        return super.build(cdcBuilder, env, customTableEnvironment, dataStreamSource);
    }
}
