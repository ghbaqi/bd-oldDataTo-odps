package com.ppio.data.transfer.old_data_to_odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static com.ppio.data.transfer.old_data_to_odps.TestGetDataFromInfluxdb.GET_DATA_FROM_INFLUX_SQL;

@Slf4j
@SpringBootTest
@RunWith(SpringRunner.class)
class OldDataToOdpsTests {


    @Resource
    private InfluxDB db;

    @Test
    public void t1() throws Exception {


        // 该部分为准备工作，仅需执行一次。
        Account account = new AliyunAccount("LTAI4GFy8z6y9KYpuGUpJQRU", "aCSMTvu2vJP2rfSFWYSxxgJUzevAr1");
        Odps odps = new Odps(account);
        odps.setEndpoint("http://service.cn-hangzhou.maxcompute.aliyun.com/api");
        odps.setDefaultProject("ppio_env_test_dev");
        TableTunnel tunnel = new TableTunnel(odps);


        db.setDatabase("telegraf");

        // 要传输数据的  起始  结束 日期
        String startDateStr = "2020-12-01";
        String endDateStr = "2020-12-31";

        LocalDate startDate = LocalDate.of(Integer.valueOf(startDateStr.substring(0, 4)), Integer.valueOf(startDateStr.substring(5, 7)), Integer.valueOf(startDateStr.substring(8, 10)));
        LocalDate endDate = LocalDate.of(Integer.valueOf(endDateStr.substring(0, 4)), Integer.valueOf(endDateStr.substring(5, 7)), Integer.valueOf(endDateStr.substring(8, 10)));

        String sql;

//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        while (startDate.compareTo(endDate) <= 0) {

            String curDateStr = startDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

            String nextDateStr = startDate.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

            // time >= '2020-12-10T00:00:00Z'  and  time < '2020-12-11T00:00:00Z'
            sql = String.format(GET_DATA_FROM_INFLUX_SQL, curDateStr, nextDateStr);
            log.info("curDateStr = {} , sql = {}", curDateStr, sql);


            Query query = new Query(sql);
            QueryResult queryResult = db.query(query);

            QueryResult.Result result = queryResult.getResults().get(0);

            if (result == null) {
                startDate = startDate.plusDays(1);
                continue;
            }

            List<QueryResult.Series> seriesList = result.getSeries();
            if (seriesList == null || seriesList.size() == 0) {
                startDate = startDate.plusDays(1);
                continue;
            }


            PartitionSpec partition = new PartitionSpec();
            partition.set("dt", curDateStr.replace("-", ""));   // 以日期为分区  ， 一天的数据在一个分区
            Table table = odps.tables().get("t_old_net_5m");
            if (!table.hasPartition(partition)) {
                table.createPartition(partition);
            }
            TableTunnel.UploadSession session = tunnel.createUploadSession("ppio_env_test_dev", "t_old_net_5m", partition);
            Record record = session.newRecord();
            RecordWriter writer = session.openBufferedWriter();

            // 可能查询多行  ， 一个 Series 代表一行
            for (QueryResult.Series series : seriesList) {

                // 查询多行  包含多行值
                List<List<Object>> lines = series.getValues();

                if (lines == null || lines.size() == 0) {
                    startDate = startDate.plusDays(1);
                    continue;
                }


//                time, depreciation, down_bandwidth, host, up_bandwidth
                log.info("数据条数 = " + lines.size());
                for (List<Object> line : lines) {
                    record.set("time", line.get(0));
                    record.set("depreciation", line.get(1));
                    record.set("down_bandwidth", line.get(2));
                    record.set("up_bandwidth", line.get(4));
                    record.set("host", line.get(3));
                    writer.write(record);
                }
            }

            writer.close();
            session.commit();
            log.info("完成了一天 = " + startDate);
            startDate = startDate.plusDays(1);

        }
    }
}
