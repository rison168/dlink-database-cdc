package com.dlink.cdc;

import com.dlink.assertion.Asserts;
import com.dlink.model.Column;
import com.dlink.model.FlinkCDCConfig;
import com.dlink.model.Schema;
import com.dlink.model.Table;
import com.dlink.executor.CustomTableEnvironment;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.*;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dlink.utils.JSONUtil;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;

/**
 * @PACKAGE_NAME: com.dlink.cdc
 * @NAME: AbstractSinkBuilder
 * @USER: Rison
 * @DATE: 2022/10/5 17:35
 * @PROJECT_NAME: dlink-database-cdc
 **/
public abstract class AbstractSinkBuilder {
    protected static final Logger logger = LoggerFactory.getLogger(AbstractSinkBuilder.class);
    protected FlinkCDCConfig config;
    protected List<ModifyOperation> modifyOperations = new ArrayList<>();

    public AbstractSinkBuilder() {
    }

    public AbstractSinkBuilder(FlinkCDCConfig config) {
        this.config = config;
    }

    public FlinkCDCConfig getConfig() {
        return config;
    }

    public void setConfig(FlinkCDCConfig config) {
        this.config = config;
    }

    protected Properties getProperties() {
        final Properties properties = new Properties();
        final Map<String, String> sink = config.getSink();
        sink.entrySet().forEach(
                entry -> {
                    if (Asserts.isNotNullString(entry.getKey()) && Asserts.isNotNullString(entry.getValue())) {
                        properties.setProperty(entry.getKey(), entry.getValue());
                    }
                }
        );
        return properties;
    }

    protected SingleOutputStreamOperator<Map> deserialize(DataStreamSource<String> dataStreamSource) {
        return dataStreamSource.map(new MapFunction<String, Map>() {
            @Override
            public Map map(String value) throws Exception {
                final ObjectMapper objectMapper = new ObjectMapper();
                return objectMapper.readValue(value, Map.class);
            }
        });
    }

    protected SingleOutputStreamOperator<Map> shunt(
            SingleOutputStreamOperator<Map> mapSingleOutputStreamOperator,
            Table table,
            String schemaFieldName
    ) {
        final String name = table.getName();
        final String schema = table.getSchema();
        return mapSingleOutputStreamOperator.filter(new FilterFunction<Map>() {
            @Override
            public boolean filter(Map map) throws Exception {
                final LinkedHashMap source = (LinkedHashMap) map.get("source");
                return name.equals(source.get(table).toString()) && schema.equals(source.get(schemaFieldName).toString());
            }
        });
    }

    protected DataStream<Map> shunt(
            SingleOutputStreamOperator singleOutputStreamOperator,
            Table table,
            OutputTag<Map> tag
    ) {
        return singleOutputStreamOperator.getSideOutput(tag);
    }

    protected DataStream<RowData> buildRowData(
            SingleOutputStreamOperator<Map> filterOperator,
            List<String> columnNameList,
            List<LogicalType> columnTypeList,
            String schemaTableName
    ) {
        return filterOperator.flatMap(new FlatMapFunction<Map, RowData>() {
            @Override
            public void flatMap(Map map, Collector<RowData> collector) throws Exception {
                try {
                    switch (map.get("op").toString()) {
                        case "r":
                        case "c":
                            GenericRowData insertGenericRowData = new GenericRowData(columnNameList.size());
                            insertGenericRowData.setRowKind(RowKind.INSERT);
                            final Map insertData = (Map) map.get("after");
                            for (int i = 0; i < columnNameList.size(); i++) {
                                insertGenericRowData.setField(i, convertValue(insertData.get(columnNameList.get(i)), columnTypeList.get(i)));
                            }
                            collector.collect(insertGenericRowData);
                            break;
                        case "d":
                            GenericRowData deleteGenericRowData = new GenericRowData(columnNameList.size());
                            deleteGenericRowData.setRowKind(RowKind.DELETE);
                            final Map deleteData = (Map) map.get("before");
                            for (int i = 0; i < columnNameList.size(); i++) {
                                deleteGenericRowData.setField(i, convertValue(deleteData.get(columnNameList.get(i)), columnTypeList.get(i)));
                            }
                            collector.collect(deleteGenericRowData);
                            break;
                        case "u":
                            GenericRowData updateBeforeGenericRowData = new GenericRowData(columnNameList.size());
                            updateBeforeGenericRowData.setRowKind(RowKind.UPDATE_BEFORE);
                            final Map updateBeforeData = (Map) map.get("before");
                            for (int i = 0; i < columnNameList.size(); i++) {
                                updateBeforeGenericRowData.setField(i, convertValue(updateBeforeData.get(columnNameList.get(i)), columnTypeList.get(i)));
                            }
                            collector.collect(updateBeforeGenericRowData);

                            GenericRowData updateAfterGenericRowData = new GenericRowData(columnNameList.size());
                            updateBeforeGenericRowData.setRowKind(RowKind.UPDATE_BEFORE);
                            final Map updateAfterData = (Map) map.get("after");
                            for (int i = 0; i < columnNameList.size(); i++) {
                                updateAfterGenericRowData.setField(i, convertValue(updateAfterData.get(columnNameList.get(i)), columnTypeList.get(i)));
                            }
                            collector.collect(updateAfterGenericRowData);
                            break;
                    }
                } catch (Exception e) {
                    logger.error("SchameTable: {} - Row: {} - Exception: {}", schemaTableName, JSONUtil.toJsonString(map), e.getCause().getMessage());
                    throw e;
                }
            }
        });

    }

    public abstract void addSink(
            StreamExecutionEnvironment env,
            DataStream<RowData> rowDataDataStream,
            Table table,
            List<String> columnNameList,
            List<LogicalType> columnTypeList);

    public DataStreamSource build(
            CDCBuilder cdcBuilder,
            StreamExecutionEnvironment env,
            CustomTableEnvironment customTableEnvironment,
            DataStreamSource<String> dataStreamSource) {

        final List<Schema> schemaList = config.getSchemaList();
        final String schemaFieldName = config.getSchemaFieldName();

        if (Asserts.isNotNullCollection(schemaList)) {
            SingleOutputStreamOperator<Map> mapOperator = deserialize(dataStreamSource);
            for (Schema schema : schemaList) {
                for (Table table : schema.getTables()) {
                    SingleOutputStreamOperator<Map> filterOperator = shunt(mapOperator, table, schemaFieldName);

                    List<String> columnNameList = new ArrayList<>();
                    List<LogicalType> columnTypeList = new ArrayList<>();

                    buildColumn(columnNameList, columnTypeList, table.getColumns());

                    DataStream<RowData> rowDataDataStream = buildRowData(filterOperator, columnNameList, columnTypeList, table.getSchemaTableName());

                    addSink(env, rowDataDataStream, table, columnNameList, columnTypeList);
                }
            }
        }
        return dataStreamSource;
    }

    protected void buildColumn(List<String> columnNameList, List<LogicalType> columnTypeList, List<Column> columns) {
        for (Column column : columns) {
            columnNameList.add(column.getName());
            columnTypeList.add(getLogicalType(column));
        }
    }

    protected LogicalType getLogicalType(Column column) {
        switch (column.getJavaType()) {
            case STRING:
                return new VarCharType();
            case BOOLEAN:
            case JAVA_LANG_BOOLEAN:
                return new BooleanType();
            case BYTE:
            case JAVA_LANG_BYTE:
                return new TinyIntType();
            case SHORT:
            case JAVA_LANG_SHORT:
                return new SmallIntType();
            case LONG:
            case JAVA_LANG_LONG:
                return new BigIntType();
            case FLOAT:
            case JAVA_LANG_FLOAT:
                return new FloatType();
            case DOUBLE:
            case JAVA_LANG_DOUBLE:
                return new DoubleType();
            case DECIMAL:
                if (column.getPrecision() == null || column.getPrecision() == 0) {
                    return new DecimalType(38, column.getScale());
                } else {
                    return new DecimalType(column.getPrecision(), column.getScale());
                }
            case INT:
            case INTEGER:
                return new IntType();
            case DATE:
            case LOCALDATE:
                return new DateType();
            case LOCALDATETIME:
            case TIMESTAMP:
                return new TimestampType();
            case BYTES:
                return new VarBinaryType(Integer.MAX_VALUE);
            default:
                return new VarCharType();
        }
    }


    protected Object convertValue(Object value, LogicalType logicalType) {
        if (value == null) {
            return null;
        }
        if (logicalType instanceof VarCharType) {
            return StringData.fromString((String) value);
        } else if (logicalType instanceof DateType) {
            return StringData.fromString(Instant.ofEpochMilli((long) value).atZone(ZoneId.systemDefault()).toLocalDate().toString());
        } else if (logicalType instanceof TimestampType) {
            return TimestampData.fromTimestamp(Timestamp.from(Instant.ofEpochMilli((long) value)));
        } else if (logicalType instanceof DecimalType) {
            final DecimalType decimalType = ((DecimalType) logicalType);
            final int precision = decimalType.getPrecision();
            final int scale = decimalType.getScale();
            return DecimalData.fromBigDecimal(new BigDecimal((String) value), precision, scale);
        } else {
            return value;
        }
    }

    protected String getSinkSchemaName(Table table) {
        String schemaName = table.getSchema();
        if (config.getSink().containsKey("sink.db")) {
            schemaName = config.getSink().get("sink.db");
        }
        return schemaName;
    }

    protected String getSinkTableName(Table table) {
        String tableName = table.getName();
        if (config.getSink().containsKey("table.prefix.schema")) {
            if (Boolean.valueOf(config.getSink().get("table.prefix.schema"))) {
                tableName = table.getSchema() + "_" + tableName;
            }
        }
        if (config.getSink().containsKey("table.prefix")) {
            tableName = config.getSink().get("table.prefix") + tableName;
        }
        if (config.getSink().containsKey("table.suffix")) {
            tableName = tableName + config.getSink().get("table.suffix");
        }
        if (config.getSink().containsKey("table.lower")) {
            if (Boolean.valueOf(config.getSink().get("table.lower"))) {
                tableName = tableName.toLowerCase();
            }
        }
        if (config.getSink().containsKey("table.upper")) {
            if (Boolean.valueOf(config.getSink().get("table.upper"))) {
                tableName = tableName.toUpperCase();
            }
        }
        return tableName;
    }

    protected List<String> getPKList(Table table) {
        List<String> pks = new ArrayList<>();
        if (Asserts.isNullCollection(table.getColumns())) {
            return pks;
        }
        for (Column column : table.getColumns()) {
            if (column.isKeyFlag()) {
                pks.add(column.getName());
            }
        }
        return pks;
    }

}
