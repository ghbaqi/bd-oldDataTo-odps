package com.ppio.data.transfer.old_data_to_odps.odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Date;

@Slf4j
@SpringBootTest
@RunWith(SpringRunner.class)
public class OdpsTest {

    /**
     * 简单连接
     */
    @Test
    public void test() {

        Account account = new AliyunAccount("LTAI4GFy8z6y9KYpuGUpJQRU", "aCSMTvu2vJP2rfSFWYSxxgJUzevAr1");
        Odps odps = new Odps(account);
        String odpsUrl = "http://service.cn-hangzhou.maxcompute.aliyun.com/api";
        odps.setEndpoint(odpsUrl);
        odps.setDefaultProject("ppio_env_test_dev");
        for (Table t : odps.tables()) {
            System.out.println(t.getName());
        }

    }

    /**
     * 上传数据
     */
    @Test
    public void t2() throws Exception {

        // 该部分为准备工作，仅需执行一次。
        Account account = new AliyunAccount("LTAI4GFy8z6y9KYpuGUpJQRU", "aCSMTvu2vJP2rfSFWYSxxgJUzevAr1");
        Odps odps = new Odps(account);
        odps.setEndpoint("http://service.cn-hangzhou.maxcompute.aliyun.com/api");
        odps.setDefaultProject("ppio_env_test_dev");
        TableTunnel tunnel = new TableTunnel(odps);

        PartitionSpec partition = new PartitionSpec();
        partition.set("dt", "20201201");
        Table table = odps.tables().get("t_old_net_5m");
        if (!table.hasPartition(partition)) {
            table.createPartition(partition);
        }


        TableTunnel.UploadSession session = tunnel.createUploadSession("ppio_env_test_dev", "t_old_net_5m", partition);
        Record record = session.newRecord();

        RecordWriter writer = session.openBufferedWriter();

        for (int i = 0; i < 1000; i++) {
            record.set("time", new Date());
            record.set("depreciation", i * 0.999);
            record.set("down_bandwidth", i * 1.11);
            record.set("up_bandwidth", i * 2.111);
            record.set("host", "mid_" + i);
            writer.write(record);
        }
        writer.close();
        session.commit();


    }
}
