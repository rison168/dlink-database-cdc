package com.dlink.model;

import lombok.Data;

import java.io.Serializable;

/**
 * @PACKAGE_NAME: com.dlink.model
 * @NAME: Column
 * @USER: Rison
 * @DATE: 2022/10/5 14:31
 * @PROJECT_NAME: dlink-database-cdc
 **/
@Data
public class Column implements Serializable {
    private static final long serialVersionUID = -1243916259987082851L;
    private String name;
    private String type;
    private String comment;
    private boolean keyFlag;
    private boolean autoIncrement;
    private String defaultValue;
    private boolean isNullable;
    private ColumnType javaType;
    private String columnFamily;
    private Integer position;
    private Integer precision;
    private Integer scale;
    private String characterSet;
    private String collation;

    public Column(String name, String type, ColumnType javaType) {this.name = name; this.type = type; this.javaType = javaType;}

    public String getFlinkType() {
        String flinkType = javaType.getFlinkType();
        if (flinkType.equals("DECIMAL")) {
            if (precision == null || precision == 0) {
                return flinkType + "(" + 38 + "," + scale + ")";
            } else {
                return flinkType + "(" + precision + "," + scale + ")";
            }
        } else {
            return flinkType;
        }
    }
}
