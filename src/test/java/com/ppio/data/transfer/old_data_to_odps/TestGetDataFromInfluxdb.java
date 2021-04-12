package com.ppio.data.transfer.old_data_to_odps;

import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.awt.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Slf4j
@SpringBootTest
@RunWith(SpringRunner.class)
public class TestGetDataFromInfluxdb {

    //    public static final String GET_DATA_FROM_INFLUX_SQL = "select  time, depreciation, down_bandwidth, host, up_bandwidth  from  five_minutes.net_5m   where  time >= '2020-12-10T00:00:00Z'  and  time < '2020-12-10T01:00:00Z'  ; ";
    public static final String GET_DATA_FROM_INFLUX_SQL = "select  time, depreciation, down_bandwidth, host, up_bandwidth  from  five_minutes.net_5m   where  time >= '%sT00:00:00Z'  and  time < '%sT00:00:00Z'  ; ";


    @Resource
    private InfluxDB db;

    /**
     * 组装 拉取的  SQL
     * 一次拉取一天的
     */
    @Test
    public void t0() {
        String startDateStr = "2020-12-01";
        String endDateStr = "2020-12-01";

        LocalDate startDate = LocalDate.of(Integer.valueOf(startDateStr.substring(0, 4)), Integer.valueOf(startDateStr.substring(5, 7)), Integer.valueOf(startDateStr.substring(8, 10)));
        LocalDate endDate = LocalDate.of(Integer.valueOf(endDateStr.substring(0, 4)), Integer.valueOf(endDateStr.substring(5, 7)), Integer.valueOf(endDateStr.substring(8, 10)));

        String sql;

        while (startDate.compareTo(endDate) <= 0) {

            String curDateStr = startDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            String nextDateStr = startDate.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

            // time >= '2020-12-10T00:00:00Z'  and  time < '2020-12-11T00:00:00Z'
            sql = String.format(GET_DATA_FROM_INFLUX_SQL, curDateStr, nextDateStr);
            log.info("curDateStr = {} , sql = {}", curDateStr, sql);

            startDate = startDate.plusDays(1);
        }
    }

}
