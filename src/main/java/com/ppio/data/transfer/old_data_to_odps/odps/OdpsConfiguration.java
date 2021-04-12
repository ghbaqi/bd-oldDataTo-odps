package com.ppio.data.transfer.old_data_to_odps.odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import sun.rmi.runtime.Log;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class OdpsConfiguration {

    private Odps odps;

    private TableTunnel tunnel;

    @Value("${odps.accessId}")
    private String accessId;

    @Value("${odps.accessKey}")
    private String accessKey;

    @Value("${odps.endPoint}")
    private String endPoint;

    @Value("${odps.project}")
    private String project;


    @Value("${influx.measurements}")
    private String influxTablesStr;

    private Map<String, Table> tableMap;

    private Map<String, TableSchema> tableSchemaMap;


    @PostConstruct
    private void init() {
        // 该部分为准备工作，仅需执行一次。
        Account account = new AliyunAccount(accessId, accessKey);
        odps = new Odps(account);
        odps.setEndpoint(endPoint);
        odps.setDefaultProject(project);
        tunnel = new TableTunnel(odps);

        tableSchemaMap = new HashMap<>();
        tableMap = new HashMap<>();

        String[] tablesStrArray = influxTablesStr.split(",");
        if (tablesStrArray == null || tablesStrArray.length == 0) {
            throw new RuntimeException("没有传入要同步的表");
        }
        for (String tName : tablesStrArray) {
            Table table = odps.tables().get(tName.trim());
            if (table == null)
                throw new RuntimeException("表不存在 table = " + tName.trim());
            tableMap.put(tName.trim(), table);
            tableSchemaMap.put(tName.trim(), table.getSchema());
        }

    }

//    public Odps getOdps() {
//        return odps;
//    }

    public Table getTable(String tableName) {
        return tableMap.get(tableName);
    }

    public TableSchema getTableSchema(String tableName) {
        return tableSchemaMap.get(tableName);
    }

    public TableTunnel getTunnel() {
        return tunnel;
    }
}
