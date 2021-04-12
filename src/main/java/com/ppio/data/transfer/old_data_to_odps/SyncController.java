package com.ppio.data.transfer.old_data_to_odps;

import org.omg.PortableInterceptor.SUCCESSFUL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SyncController {

    @Autowired
    private DataSyncService dataSyncService;


    @GetMapping("/sync")
    public String sync(String tableName, String startDateStr, String endDateStr) {

        if (tableName == null || tableName.trim().equals("")) {
            return "非法的表名称";
        }
        tableName = tableName.trim().toLowerCase();

        if (startDateStr == null || startDateStr.trim().equals(""))
            return "error 非法的参数";
        if (endDateStr == null || endDateStr.trim().equals(""))
            endDateStr = startDateStr;

        dataSyncService.sync(tableName, startDateStr, endDateStr);

        return "success";
    }
}
