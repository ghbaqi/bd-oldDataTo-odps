package com.ppio.data.transfer.old_data_to_odps;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.ppio.data.transfer.old_data_to_odps.odps.OdpsConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.influxdb.InfluxDBTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;

@Slf4j
@Service
public class DataSyncService {

    public static final String INFLUX_NET_5M_SQL = "select  time, depreciation, down_bandwidth, host, up_bandwidth  from  five_minutes.net_5m   where  time >= '%sT16:00:00Z'  and  time < '%sT16:00:00Z'; ";

    public static final String INFLUX_CPU_5M_SQL = "select  time, cpu , host, usage_active  from  five_minutes.cpu_5m   where  time >= '%sT16:00:00Z'  and  time < '%sT16:00:00Z'; ";

    public static final String INFLUX_DISK_5M_SQL = "select  time, device , host, increment , total , used , used_percent  from  five_minutes.disk_5m   where  time >= '%sT16:00:00Z'  and  time < '%sT16:00:00Z'; ";

    public static final String INFLUX_DOCKER_CONTAINER_NET_5M_SQL = "select  time, container_name , container_version, down_bandwidth , host , network , up_bandwidth  from  five_minutes.docker_container_net_5m   where  time >= '%sT16:00:00Z'  and  time < '%sT16:00:00Z'; ";

    public static final String INFLUX_MEM_5M_SQL = "select  time, host , total, used , used_percent from  five_minutes.mem_5m   where  time >= '%sT16:00:00Z'  and  time < '%sT16:00:00Z'; ";

    public static final String INFLUX_NET_5M_ORIGIN_SQL = "select  time, depreciation, down_bandwidth , host , up_bandwidth  from  five_minutes.net_5m_origin   where  time >= '%sT16:00:00Z'  and  time < '%sT16:00:00Z'; ";

    public static final String INFLUX_NET_5M_ORIGIN_DOWN_BANDWIDTH_SQL = "select  time, depreciation , down_bandwidth ,host , up_bandwidth    from  five_minutes.net_5m_origin_down_bandwidth   where  time >= '%sT16:00:00Z'  and  time < '%sT16:00:00Z'; ";

    public static final String INFLUX_PI_FLOW_ROBOT_5M_SQL = "select  time, host , up_bandwidth   from  five_minutes.pi_flow_robot_5m   where  time >= '%sT16:00:00Z'  and  time < '%sT16:00:00Z'; ";

    public static final String INFLUX_PROCSTAT_LOOKUP_5M_SQL = "select  time,  dcache_id , host ,  pattern ,  running   from  five_minutes.procstat_lookup_5m   where  time >= '%sT16:00:00Z'  and  time < '%sT16:00:00Z'; ";

//> select *  from   five_minutes.bandwidth_compensation   where  time >= '2021-03-09T00:00:00Z'   limit  2;
//    name: bandwidth_compensation
//    time                 host                             up_bandwidth
//----                 ----                             ------------
//        2021-03-09T11:15:00Z 12494f8f87ab4ab9a0a271cf6fa4f82f 2511.102028143421
//        2021-03-09T11:15:00Z 16c0d05384600b981608430f4b4c4095 491.60247056945303


    public static final String INFLUX_BANDWIDTH_COMPENSATION_SQL = "select  time, host , up_bandwidth   from  five_minutes.bandwidth_compensation   where  time >= '%sT16:00:00Z'  and  time < '%sT16:00:00Z'; ";


//    @Resource
//    private InfluxDB db;

    @Autowired
    private InfluxDBTemplate<Point> influxDBTemplate;

    @Autowired
    private OdpsConfiguration odpsConfiguration;

    @Value("${odps.project}")
    private String project;

    /**
     * 同步数据  influx -->  odps
     */
    @Async
    public String sync(String tableName, String startDateStr, String endDateStr) {

//        db.setDatabase("telegraf");

        // 要传输数据的  起始  结束 日期
//        String startDateStr = "2020-12-01";
//        String endDateStr = "2020-12-31";
        LocalDate startDate;
        LocalDate endDate;
        try {
            // 传入的日期都表示  东八区 日期时间
            startDate = LocalDate.of(Integer.valueOf(startDateStr.substring(0, 4)), Integer.valueOf(startDateStr.substring(5, 7)), Integer.valueOf(startDateStr.substring(8, 10)));
            endDate = LocalDate.of(Integer.valueOf(endDateStr.substring(0, 4)), Integer.valueOf(endDateStr.substring(5, 7)), Integer.valueOf(endDateStr.substring(8, 10)));
        } catch (Exception e) {
            log.error("传入的日期格式有误 , startDateStr ={} , endDateStr = {}", startDateStr, endDateStr);
            return "failed 传入的日期格式有误";
        }


        String sql;

//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        while (startDate.compareTo(endDate) <= 0) {


            try {

                String curDateStr = startDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                String preDateStr = startDate.plusDays(-1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

                /**
                 * 我想查 当天的数据 ， 比如想查 14 号的数据 。
                 * 那么在 influx 中查询范围是 13号 16 点 到 14 号 16 点
                 */
                // time >= '2020-12-10T00:00:00Z'  and  time < '2020-12-11T00:00:00Z'

                sql = getInfluxQuerySql(tableName, preDateStr, curDateStr);
//                sql = String.format(INFLUX_NET_5M_SQL, preDateStr, curDateStr);
                log.info("tableName = {} ,  curDateStr = {} , sql = {}", tableName, curDateStr, sql);

                Query query = new Query(sql, "telegraf");
                QueryResult queryResult = influxDBTemplate.query(query);

//                QueryResult queryResult = db.query(query);


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
                Table table = odpsConfiguration.getTable(tableName);
                log.info("tableName= {} , table = {} ", tableName, table);
                if (!table.hasPartition(partition)) {
                    table.createPartition(partition);
                } else {
                    table.deletePartition(partition);
                    table.createPartition(partition);
                }
                TableTunnel.UploadSession session = odpsConfiguration.getTunnel().createUploadSession(project, tableName, partition);
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
                        assembleRecord(tableName, line, record);
                        writer.write(record);
                    }
                }

                writer.close();
                session.commit();
                log.info("完成了一天 = " + startDate);
                startDate = startDate.plusDays(1);

            } catch (Exception e) {
                log.error("同步数据错误 , 日期 = {} ，error = {}", startDate, e);
                return "同步错误, 日期 = " + startDate + " , error = " + e;
            }

        }

        return "success";

    }

    /**
     * 将 influx 中的一行记录  填充到 odps 的一个 record
     *
     * @param tableName
     * @param line
     * @param record
     */
    private void assembleRecord(String tableName, List<Object> line, Record record) {

        switch (tableName) {
            case "t_old_net_5m":   // time, depreciation, down_bandwidth, host, up_bandwidth
                record.set("time", line.get(0));
                record.set("depreciation", line.get(1));
                record.set("down_bandwidth", line.get(2));
                record.set("up_bandwidth", line.get(4));
                record.set("host", line.get(3));
                break;
            case "t_old_cpu_5m":   //  time, cpu , host, usage_active
                record.set("time", line.get(0));
                record.set("cpu", line.get(1));
                record.set("host", line.get(2));
                record.set("usage_active", line.get(3));
                break;
            case "t_old_disk_5m":   //   time, device , host, increment , total , used , used_percent
                record.set("time", line.get(0));
                record.set("device", line.get(1));
                record.set("host", line.get(2));
                record.set("increment", line.get(3));
                record.set("total", line.get(4));
                record.set("used", line.get(5));
                record.set("used_percent", line.get(6));
                break;
            case "t_old_docker_container_net_5m":   // time   container_name   container_version down_bandwidth     host      network up_bandwidth
                record.set("time", line.get(0));
                record.set("container_name", line.get(1));
                record.set("container_version", line.get(2));
                record.set("down_bandwidth", line.get(3));
                record.set("host", line.get(4));
                record.set("network", line.get(5));
                record.set("up_bandwidth", line.get(6));
                break;
            case "t_old_mem_5m":   //   time, host , total, used , used_percent
                record.set("time", line.get(0));
                record.set("host", line.get(1));
                record.set("total", line.get(2));
                record.set("used", line.get(3));
                record.set("used_percent", line.get(4));

                break;
            case "t_old_net_5m_origin":   // time, depreciation, down_bandwidth , host , up_bandwidth
                record.set("time", line.get(0));
                record.set("depreciation", line.get(1));
                record.set("down_bandwidth", line.get(2));
                record.set("host", line.get(3));
                record.set("up_bandwidth", line.get(4));
                break;
            case "t_old_net_5m_origin_down_bandwidth":   //  time, depreciation , down_bandwidth ,host , up_bandwidth
                record.set("time", line.get(0));
                record.set("depreciation", line.get(1));
                record.set("down_bandwidth", line.get(2));
                record.set("host", line.get(3));
                record.set("up_bandwidth", line.get(4));
                break;
            case "t_old_pi_flow_robot_5m":   //  time                 host                             up_bandwidth  from  five_minutes.pi_flow_robot_5m
                record.set("time", line.get(0));
                record.set("host", line.get(1));
                record.set("up_bandwidth", line.get(2));
                break;
            case "t_old_procstat_lookup_5m":   //  time,  dcache_id , host ,  pattern ,  running
                record.set("time", line.get(0));
                record.set("dcache_id", line.get(1));
                record.set("host", line.get(2));
                record.set("pattern", line.get(3));
                record.set("running", line.get(4));

                break;
            case "t_ods_machine_compensation_plan":   //  time, host , up_bandwidth
                String time = (String) line.get(0);
                LocalDateTime localDateTime = LocalDateTime.parse(time, DateTimeFormatter.ISO_DATE_TIME).plusHours(8L);
                ZoneId zoneId = ZoneId.systemDefault();
                ZonedDateTime zdt = localDateTime.atZone(zoneId);
                Date date = Date.from(zdt.toInstant());
                record.set("time", date);
                record.set("machine_id", line.get(1));
                double up_bandwidth = (double) line.get(2);
                long up_bandwidth2 = (long) (up_bandwidth * 1024 * 1024);
                record.set("up_bandwidth", up_bandwidth2);
                String timeId = localDateTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
                record.set("time_id", timeId);

                break;
            default:
                throw new RuntimeException("未知的表名");
        }


    }

    public static void main(String[] args) {
        String d = "2021-03-10T11:20:00Z";
        LocalDateTime date = LocalDateTime.parse(d, DateTimeFormatter.ISO_DATE_TIME).plusHours(8L);
        System.out.println(date);
        String timeId = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
        System.out.println(timeId);
    }

    private String getInfluxQuerySql(String tableName, String... args) {
        String sql = "";

        switch (tableName) {
            case "t_old_net_5m":
                //  time >= '2020-12-10T00:00:00Z'  and  time < '2020-12-11T00:00:00Z'
                sql = String.format(INFLUX_NET_5M_SQL, args[0], args[1]);
                break;
            case "t_old_cpu_5m":
                sql = String.format(INFLUX_CPU_5M_SQL, args[0], args[1]);
                break;
            case "t_old_disk_5m":
                sql = String.format(INFLUX_DISK_5M_SQL, args[0], args[1]);
                break;
            case "t_old_docker_container_net_5m":
                sql = String.format(INFLUX_DOCKER_CONTAINER_NET_5M_SQL, args[0], args[1]);
                break;
            case "t_old_mem_5m":
                sql = String.format(INFLUX_MEM_5M_SQL, args[0], args[1]);
                break;
            case "t_old_net_5m_origin":
                sql = String.format(INFLUX_NET_5M_ORIGIN_SQL, args[0], args[1]);
                break;
            case "t_old_net_5m_origin_down_bandwidth":
                sql = String.format(INFLUX_NET_5M_ORIGIN_DOWN_BANDWIDTH_SQL, args[0], args[1]);
                break;
            case "t_old_pi_flow_robot_5m":
                sql = String.format(INFLUX_PI_FLOW_ROBOT_5M_SQL, args[0], args[1]);
                break;
            case "t_old_procstat_lookup_5m":
                sql = String.format(INFLUX_PROCSTAT_LOOKUP_5M_SQL, args[0], args[1]);
                break;
            case "t_ods_machine_compensation_plan":
                sql = String.format(INFLUX_BANDWIDTH_COMPENSATION_SQL, args[0], args[1]);
                break;

            default:
                log.error("未知的 table = " + tableName);
                throw new RuntimeException("非法的表名称");
        }

        return sql;
    }
}
