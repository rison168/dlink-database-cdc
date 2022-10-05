package com.dlink.model;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @PACKAGE_NAME: com.dlink.model
 * @NAME: Schema
 * @USER: Rison
 * @DATE: 2022/10/5 14:12
 * @PROJECT_NAME: dlink-database-cdc
 **/
@Data
public class Schema implements Serializable, Comparable<Schema> {
    private static final long serialVersionUID = -5022000045175697491L;
    private String name;
    private List<Table> tables = new ArrayList<>();
    private List<String> views = new ArrayList<>();
    private List<String> functions = new ArrayList<>();
    private List<String> userFunctions = new ArrayList<>();
    private List<String> modules = new ArrayList<>();

    public Schema(String name){this.name = name;}

    public Schema(String name, List<Table> tables){
        this.name = name;
        this.tables = tables;
    }

    @Override
    public int compareTo(Schema o) {
        return this.name.compareTo(o.getName());
    }
}
