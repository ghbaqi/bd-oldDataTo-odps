package com.ppio.data.transfer.old_data_to_odps;

import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.runner.RunWith;
import org.springframework.boot.actuate.endpoint.invoke.convert.ConversionServiceParameterValueMapper;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Slf4j
@SpringBootTest
@RunWith(SpringRunner.class)
public class Test {


    @Resource
    private InfluxDB db;

    /**
     * 简单查询两行
     */
    @org.junit.Test
    public void test2() throws ParseException {
        db.setDatabase("telegraf");
        Query query = new Query("select  time, depreciation, down_bandwidth, host, up_bandwidth from  five_minutes.net_5m   where  time > '2020-12-11'  limit 12");
        QueryResult queryResult = db.query(query);

        QueryResult.Result result = queryResult.getResults().get(0);

//        System.out.println(result.toString());

        List<QueryResult.Series> seriesList = result.getSeries();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");


        // 可能查询多行  ， 一个 Series 代表一行
        for (QueryResult.Series series : seriesList) {

            // 查询多行  包含多行值
            List<List<Object>> lines = series.getValues();

            for (List<Object> line : lines) {
                log.info("time = {} , depreciation = {} ,  down_bandwidth = {} , host = {} ,  up_bandwidth = {} "
//                        , dateFormat.parse(line.get(0).toString())
                        , line.get(0).toString()
                        , line.get(1)
                        , line.get(2)
                        , line.get(3)
                        , line.get(4)
                );
            }


        }

    }


    /**
     * 查询一个小时的数据
     */
    @org.junit.Test
    public void test3() {
        db.setDatabase("telegraf");
        Query query = new Query("select  time, depreciation, down_bandwidth, host, up_bandwidth  from  five_minutes.net_5m   where  time >= '2020-12-01T00:00:00Z'  and  time < '2020-12-02T00:00:00Z'  ; ");
        QueryResult queryResult = db.query(query);

        QueryResult.Result result = queryResult.getResults().get(0);

//        System.out.println(result.toString());

        List<QueryResult.Series> seriesList = result.getSeries();

        // 可能查询多行  ， 一个 Series 代表一行
        for (QueryResult.Series series : seriesList) {

            // 查询多行  包含多行值
            List<List<Object>> lines = series.getValues();

            log.info("数据总条数为 = " + lines.size());
            for (List<Object> line : lines) {
                log.info("time = {} , depreciation = {} ,  down_bandwidth = {} , host = {} ,  up_bandwidth = {} "
                        , line.get(0)
                        , line.get(1)
                        , line.get(2)
                        , line.get(3)
                        , line.get(4)
                );
            }
        }
    }

    public static void main(String[] args) throws ParseException {
        System.out.println("2020-12-10T00:35:00Z");

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        Date date = dateFormat.parse("2020-12-10T00:35:00Z");
        System.out.println(date);    // Thu Dec 10 00:35:00 CST 2020

        System.out.println(new Date());
    }


}
