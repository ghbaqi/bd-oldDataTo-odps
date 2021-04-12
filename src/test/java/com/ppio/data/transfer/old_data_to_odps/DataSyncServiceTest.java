package com.ppio.data.transfer.old_data_to_odps;

import lombok.extern.slf4j.Slf4j;
import org.influxdb.dto.Point;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.influxdb.InfluxDBTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@Slf4j
@SpringBootTest
@RunWith(SpringRunner.class)
public class DataSyncServiceTest {

    @Autowired
    DataSyncService dataSyncService;

    @Test
    public void test1() throws InterruptedException {
        dataSyncService.sync("t_old_net_5m", "2020-12-01", "2020-12-05");
        Thread.sleep(1000000L);
    }


    @Test
    public void test2() throws InterruptedException {
        dataSyncService.sync("t_old_cpu_5m", "2020-12-01", "2020-12-05");
        Thread.sleep(1000000L);
    }

    @Test
    public void test3() throws InterruptedException {
        dataSyncService.sync("t_old_disk_5m", "2020-12-01", "2020-12-05");
        Thread.sleep(1000000L);
    }

    @Test
    public void test4() throws InterruptedException {
        dataSyncService.sync("t_old_docker_container_net_5m", "2020-12-01", "2020-12-05");
        Thread.sleep(1000000L);
    }

    @Test
    public void test5() throws InterruptedException {
        dataSyncService.sync("t_old_mem_5m", "2020-12-01", "2020-12-05");
        Thread.sleep(1000000L);
    }

    @Test
    public void test6() throws InterruptedException {
        dataSyncService.sync("t_old_net_5m_origin", "2020-12-01", "2020-12-05");
        Thread.sleep(1000000L);
    }

    @Test
    public void test7() throws InterruptedException {
        dataSyncService.sync("t_old_net_5m_origin_down_bandwidth", "2020-11-01", "2020-12-16");
        Thread.sleep(1000000L);
    }


    @Test
    public void test8() throws InterruptedException {
        dataSyncService.sync("t_old_pi_flow_robot_5m", "2020-12-01", "2020-12-16");
        Thread.sleep(1000000L);
    }


    @Test
    public void test9() throws InterruptedException {
        dataSyncService.sync("t_old_procstat_lookup_5m", "2020-12-01", "2020-12-05");
        Thread.sleep(1000000L);
    }

}
