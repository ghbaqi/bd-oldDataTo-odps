package com.ppio.data.transfer.old_data_to_odps;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;


/**
 * mvn  clean  package  -Dmaven.test.skip=true
 * 运行命令
 * <p>
 *
 * 测试环境
 * nohup java -jar -Dspring.profiles.active=test    old_data_to_odps-0.0.1-SNAPSHOT.jar  > /dev/null 2>&1  &
 *
 * 生产环境
 * nohup java -jar -Dspring.profiles.active=prd    old_data_to_odps-0.0.1-SNAPSHOT.jar   > /dev/null 2>&1  &
 *
 * <p>
 * <p>
 * 向生产机器传输 jar  包
 * jvm 参数 :
 *
 *
 * wget https://pi-platform-deploy.oss-cn-hangzhou-internal.aliyuncs.com/bigdata/old_data_to_odps-0.0.1-SNAPSHOT.jar
 *
 *
 * 触发同步接口
 * curl   '127.0.0.1:9950/sync?startDateStr=2020-03-01&endDateStr=2020-03-31&tableName=t_old_net_5m'
 *
 * curl   '127.0.0.1:9950/sync?startDateStr=2021-03-10&endDateStr=2021-03-10&tableName=t_old_net_5m'
 * curl   '127.0.0.1:9950/sync?startDateStr=2021-03-10&endDateStr=2021-03-10&tableName=t_ods_machine_compensation_plan'
 *
 */


/**
 * 20210311  将打量数据从  influx  中同步 ， 原来kafka 那边同步停掉
 * 1. old_data_to_odps  重启
 * 2. bd_kafka_to_odps 重启
 */
@EnableAsync
@EnableScheduling
@SpringBootApplication
public class OldDataToOdpsApplication {

    public static void main(String[] args) {
        SpringApplication.run(OldDataToOdpsApplication.class, args);
    }

}
