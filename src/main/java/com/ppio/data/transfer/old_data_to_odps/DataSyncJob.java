package com.ppio.data.transfer.old_data_to_odps;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.Date;

@Slf4j
@Service
public class DataSyncJob {

    @Autowired
    private DataSyncService dataSyncService;

    /**
     * 每日凌晨 2. 同步前一天的数据
     */

    // net_5m
    @Scheduled(cron = "0 0 2 1/1 * ? ")
    public void syncDaily_net_5m() {
        LocalDate bizDate = LocalDate.now().plusDays(-1);
        log.info("定时同步 net_5m bizDate = " + bizDate);
        dataSyncService.sync("t_old_net_5m", bizDate.toString(), bizDate.toString());
    }

    // cpu_5m
    @Scheduled(cron = "0 10 2 1/1 * ? ")
    public void syncDaily_cpu_5m() {
        LocalDate bizDate = LocalDate.now().plusDays(-1);
        log.info("定时同步 cpu_5m bizDate = " + bizDate);
        dataSyncService.sync("t_old_cpu_5m", bizDate.toString(), bizDate.toString());
    }

    // disk_5m
    @Scheduled(cron = "0 15 2 1/1 * ? ")
    public void syncDaily_disk_5m() {
        LocalDate bizDate = LocalDate.now().plusDays(-1);
        log.info("定时同步 disk_5m bizDate = " + bizDate);
        dataSyncService.sync("t_old_disk_5m", bizDate.toString(), bizDate.toString());
    }

    @Scheduled(cron = "0 20 2 1/1 * ? ")
    public void syncDaily_docker_container_net_5m() {
        LocalDate bizDate = LocalDate.now().plusDays(-1);
        log.info("定时同步 docker_container_net_5m bizDate = " + bizDate);
        dataSyncService.sync("t_old_docker_container_net_5m", bizDate.toString(), bizDate.toString());
    }

    @Scheduled(cron = "0 25 2 1/1 * ? ")
    public void syncDaily_mem_5m() {
        LocalDate bizDate = LocalDate.now().plusDays(-1);
        log.info("定时同步 mem_5m bizDate = " + bizDate);
        dataSyncService.sync("t_old_mem_5m", bizDate.toString(), bizDate.toString());
    }

    @Scheduled(cron = "0 30 2 1/1 * ? ")
    public void syncDaily_net_5m_origin() {
        LocalDate bizDate = LocalDate.now().plusDays(-1);
        log.info("定时同步 net_5m_origin bizDate = " + bizDate);
        dataSyncService.sync("t_old_net_5m_origin", bizDate.toString(), bizDate.toString());
    }

    @Scheduled(cron = "0 35 2 1/1 * ? ")
    public void syncDaily_net_5m_origin_down_bandwidth() {
        LocalDate bizDate = LocalDate.now().plusDays(-1);
        log.info("定时同步 net_5m_origin_down_bandwidth bizDate = " + bizDate);
        dataSyncService.sync("t_old_net_5m_origin_down_bandwidth", bizDate.toString(), bizDate.toString());
    }

    @Scheduled(cron = "0 40 2 1/1 * ? ")
    public void syncDaily_pi_flow_robot_5m() {
        LocalDate bizDate = LocalDate.now().plusDays(-1);
        log.info("定时同步 pi_flow_robot_5m bizDate = " + bizDate);
        dataSyncService.sync("t_old_pi_flow_robot_5m", bizDate.toString(), bizDate.toString());
    }

    @Scheduled(cron = "0 45 2 1/1 * ? ")
    public void syncDaily_procstat_lookup_5m() {
        LocalDate bizDate = LocalDate.now().plusDays(-1);
        log.info("定时同步 procstat_lookup_5m bizDate = " + bizDate);
        dataSyncService.sync("t_old_procstat_lookup_5m", bizDate.toString(), bizDate.toString());
    }


    /**
     * 原来打量数据通过 kafka 同步 ，
     * 现在改为从 influx  中同步 。和老数据不同 这部分在 proxy 库中
     * 老数据在  telegraf 库中
     */
    @Scheduled(cron = "0 30 2 1/1 * ? ")
    public void syncOds_machine_compensation_plan() {
        LocalDate bizDate = LocalDate.now().plusDays(-1);
        log.info("定时同步 t_ods_machine_compensation_plan bizDate = " + bizDate);
        dataSyncService.sync("t_ods_machine_compensation_plan", bizDate.toString(), bizDate.toString());
    }
}
