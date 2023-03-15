package com.iotmars.compass;

import com.iotmars.compass.udf.ParseErrorCodeAndGetDiff;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author CJ
 * @date: 2022/11/3 19:48
 */
public class DeviceLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置checkpoint
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(90 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1L), Time.minutes(1L)
        ));

        // 设置StateBackend
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        System.setProperty("HADOOP_USER_NAME", "root");
        env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.101.193:8020/flink/checkpoint/DWS_DEVICE_FEIYAN_LOG_EVENT");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        TableConfig config = tableEnv.getConfig();

        config.getConfiguration().setString("parallelism.default", "3");
        config.getConfiguration().setString("table.exec.state.ttl", String.valueOf(24 * 60 * 60 * 1000)); // 如果state状态24小时不变，则回收。需要根据后面的逻辑调整。
        config.getConfiguration().setString("table.exec.source.idle-timeout", String.valueOf(30 * 1000)); // 源在超时时间3000ms内未收到任何元素时，它将被标记为暂时空闲。这允许下游任务推进其水印.

        // 读取dwd层数据
        tableEnv.executeSql("CREATE TABLE dwd_device_feiyan_log_event (" +
                "   deviceType string," +
                "   iotId string," +
                "   requestId string," +
                "   checkFailedData string," +
                "   productKey string," +
                "   gmtCreate bigint," +
                "   ts_ltz AS TO_TIMESTAMP_LTZ(gmtCreate, 3)," +
                "   deviceName string," +
                "   eventName string," +
                "   eventValue string," +
                "   eventOriValue string," +
                "   proctime AS PROCTIME()" +
                ") WITH (" +
                "   'connector' = 'kafka', " +
                "   'topic' = 'dwd_device_feiyan_log_event', " +
                "   'properties.bootstrap.servers' = '192.168.101.193:9092,192.168.101.194:9092,192.168.101.195:9092', " +
                "   'properties.group.id' = 'flink-compass-device', " +
//                "   'scan.startup.mode' = 'earliest-offset', " +
//                "   'scan.startup.mode' = 'latest-offset', " +
                "   'scan.startup.mode' = 'group-offsets', " +
                "   'format' = 'json'" +
                ")");

        // 读取维度表数据
        tableEnv.executeSql("CREATE TABLE dim_pub_smart_product_fault_dict (" +
                "   fault_type_code string," +
                "   smart_product_code string," +
                "   fault_code string," +
                "   fault_name string" +
                ") WITH (" +
                "   'connector'='jdbc'," +
                "   'url'='jdbc:mysql://rm-bp1n6ikz7s1gk33q1mo.mysql.rds.aliyuncs.com:3306/compass_prod'," +
                "   'table-name'='bb_smart_product_fault_dict'," +
                "   'username'='compass'," +
                "   'password'='Compass@123!'" +
                ")");

        // 处理数据
        tableEnv.createTemporaryFunction("parseErrorCodeAndGetDiff", ParseErrorCodeAndGetDiff.class);

        tableEnv.executeSql("CREATE VIEW tmp_fault AS ( " +
                "   SELECT tmp_fault.productKey, " +
                "           tmp_fault.iotId, " +
                "           tmp_fault.deviceName, " +
                "           tmp_fault.ts_ltz, " +
                "           tmp_fault.eventName, " +
                "           tmp_dict.fault_code as optId, " +
                "           tmp_dict.fault_name as optName " +
                "    FROM ( " +
                "             SELECT * " +
                "             FROM ( " +
                "                      SELECT proctime," +
                "                             iotId, " +
                "                             requestId, " +
                "                             productKey, " +
                "                             ts_ltz, " +
                "                             deviceName, " +
                "                             'ErrorCode'                                        as eventName, " +
                "                             str_to_map(eventValue, ',', ':')['ErrorCode']     as event_real_value, " +
                "                             str_to_map(eventOriValue, ',', ':')['ErrorCode'] as event_real_ori_value " +
                "                      FROM dwd_device_feiyan_log_event " +
                "                      where eventName = 'ErrorCode' " +
                "                  ) tmp_event " +
                "                      INNER JOIN LATERAL TABLE(parseErrorCodeAndGetDiff(event_real_value, event_real_ori_value)) AS T(fault_code) ON TRUE" +
                "         ) tmp_fault " +
                "             LEFT JOIN " +
                "               dim_pub_smart_product_fault_dict for SYSTEM_TIME AS OF tmp_fault.proctime AS tmp_dict" +
                "         ON concat('E', tmp_fault.fault_code) = tmp_dict.fault_code and " +
                "            tmp_fault.productKey = tmp_dict.smart_product_code " +
//                "         -- 匹配不上异常字典表的异常过滤，如Q6BC的E12故障 " +
                "    WHERE tmp_dict.fault_type_code is not null " +
                ")");

        tableEnv.executeSql("CREATE VIEW tmp_ov_mode AS ( " +
                "    SELECT productKey, iotId, deviceName, ts_ltz, eventName, ov_mode as optId, ov_name as optName " +
                "    FROM ( " +
                "             SELECT productKey, " +
                "                    iotId, " +
                "                    deviceName, " +
                "                    ts_ltz, " +
                "                    'StOvMode'                                               as eventName, " +
                "                    coalesce(str_to_map(eventValue, ',', ':')['StOvMode'], " +
                "                             str_to_map(eventValue, ',', ':')['LStOvMode'], " +
                "                             str_to_map(eventValue, ',', ':')['RStOvMode']) as ov_mode, " +
                "                    cast(null as string)                                     as ov_name " +
                "             FROM dwd_device_feiyan_log_event " +
                "             where eventName in ('StOvMode', 'LStOvMode', 'RStOvMode') " +
//                "               -- 如果最新值是0，表示关闭工作模式，筛除该条数据 " +
                "               and str_to_map(eventValue, ',', ':')['StOvMode'] <> '0' " +
//                "               -- 不包括智慧菜谱联动的工作模式 " +
                "               and (str_to_map(eventValue, ',', ':')['MultiStageName'] = '' " +
                "                 or str_to_map(eventValue, ',', ':')['MultiStageName'] is null) " +
                "         ) tmp " +
                ")");

        tableEnv.executeSql("CREATE VIEW tmp_multistage_name AS ( " +
                "    SELECT productKey, iotId, deviceName, ts_ltz, eventName, cookbook_id as optId, cookbook_name as optName " +
                "    FROM ( " +
                "             SELECT productKey, " +
                "                    iotId, " +
                "                    deviceName, " +
                "                    ts_ltz, " +
                "                    'MultiStageName'                                      as eventName, " +
//                "                    -- str_to_map(eventValue, ',', ':')['CookbookID']    as cookbook_id, " +
                "                    str_to_map(eventValue, ',', ':')['MultiStageName']   as cookbook_id, " +
                "                    str_to_map(eventValue, ',', ':')['MultiStageName']   as cookbook_name " +
                "             FROM dwd_device_feiyan_log_event " +
                "             where eventName = 'MultiStageName' " +
//                "               -- 如果最新值是0，表示关闭智慧菜谱，筛除该条数据 " +
                "               and str_to_map(eventValue, ',', ':')['MultiStageName'] <> '' " +
                "               and str_to_map(eventValue, ',', ':')['MultiStageName'] is not null " +
                "         ) tmp" +
                ")");

//        tableEnv.executeSql("CREATE TABLE LogPrintTable (" +
//                "   mark string," +
//                "   deviceType string," +
//                "   iotId string," +
//                "   requestId string," +
//                "   checkFailedData string," +
//                "   productKey string," +
//                "   ts_ltz timestamp(3)," +
//                "   deviceName string," +
//                "   eventName string," +
//                "   eventValue string," +
//                "   eventOriValue string" +
//                ") WITH (" +
//                "  'connector' = 'print'" +
//                ")");
//
//        tableEnv.executeSql("INSERT INTO LogPrintTable" +
//                "   SELECT 'log'," +
//                "       deviceType," +
//                "       iotId," +
//                "       requestId," +
//                "       checkFailedData," +
//                "       productKey," +
//                "       ts_ltz," +
//                "       deviceName," +
//                "       eventName," +
//                "       eventValue," +
//                "       eventOriValue" +
//                "   FROM dwd_device_feiyan_log_event");

        tableEnv.executeSql("CREATE TABLE MySQLSinkTable (" +
                "   ds string, " +
                "   productKey string, " +
                "   iotId string, " +
                "   deviceName string, " +
                "   eventName string, " +
                "   optId string, " +
                "   optName string, " +
                "   cnt bigint," +
                "   PRIMARY KEY(ds,iotId,eventName,optId) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://192.168.32.244:3306/device_model_log?characterEncoding=utf8'," +
                "  'table-name' = 'dws_device_feiyan_log_event'," +
                "  'username' = 'root', " +
                "  'password' = 'hxr'" +
                ")");
        tableEnv.executeSql("INSERT INTO MySQLSinkTable " +
                "   SELECT date_format(ts_ltz, 'yyyy-MM-dd') as ds, productKey, iotId, deviceName, eventName, optId, optName, count(*) as cnt " +
                "   FROM ((select * from tmp_fault) union all (select * from tmp_ov_mode) union all (select * from tmp_multistage_name)) t " +
                "   GROUP BY productKey, iotId, deviceName, eventName, optId, optName, date_format(ts_ltz, 'yyyy-MM-dd') "
        );


        // 输出到doris
//        tableEnv.executeSql("CREATE TABLE DorisSinkTable (" +
//                "   productKey string, " +
//                "   iotId string, " +
//                "   deviceName string, " +
//                "   eventName string, " +
//                "   optId string, " +
//                "   optName string, " +
//                "   cnt bigint" +
//                ") WITH (" +
//                "  'connector' = 'jdbc'," +
//                "  'url' = 'jdbc:mysql://192.168.101.242:9030/compass_device_dev'," +
//                "  'table-name' = 'dws_device_feiyan_log_event'," +
//                "  'username' = 'root', " +
//                "  'password' = '123456'" +
//                ")");

//        tableEnv.executeSql("CREATE TABLE PrintTable (" +
//                "   ds string, " +
//                "   productKey string, " +
//                "   iotId string, " +
//                "   deviceName string, " +
//                "   eventName string, " +
//                "   optId string, " +
//                "   optName string, " +
//                "   cnt bigint" +
//                ") WITH (" +
//                "  'connector' = 'print'" +
//                ")");

//        tableEnv.executeSql("INSERT INTO PrintTable " +
//                        "   SELECT date_format(ts_ltz, 'yyyy-MM-dd') as ds, productKey, iotId, deviceName, eventName, optId, optName, count(*) as cnt " +
//                        "   FROM ((select * from tmp_fault) union all (select * from tmp_ov_mode) union all (select * from tmp_multistage_name)) t " +
//                        "   GROUP BY productKey, iotId, deviceName, eventName, optId, optName, date_format(ts_ltz, 'yyyy-MM-dd') "
//        );
    }
}
