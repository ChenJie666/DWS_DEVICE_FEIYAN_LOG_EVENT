CREATE TABLE compass_device_dev.dws_device_feiyan_log_event (
    `productkey` varchar(100) NULL COMMENT "",
    `iotid` varchar(255) NULL COMMENT "",
    `devicename` varchar(100) NULL COMMENT "",
    `event_name` varchar(255) NULL COMMENT "",
    `opt_id` varchar(100) NULL COMMENT "",
    `opt_name` varchar(100) NULL COMMENT "",
    `cnt` bigint NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`productkey`, `iotId`)
COMMENT "飞燕设备日志日统计表"
DISTRIBUTED BY HASH(`productkey`) BUCKETS 4
PROPERTIES("replication_num"="2");

DROP TABLE device_model_log.dws_device_feiyan_log_event;
CREATE TABLE device_model_log.dws_device_feiyan_log_event (
	`ds` varchar(25) COMMENT '',
    `productKey` varchar(100) NULL COMMENT "",
    `iotId` varchar(255) NULL COMMENT "",
    `deviceName` varchar(100) NULL COMMENT "",
    `eventName` varchar(255) NULL COMMENT "",
    `optId` varchar(100) NULL COMMENT "",
    `optName` varchar(100) NULL COMMENT "",
    `cnt` bigint NULL COMMENT "",
	PRIMARY KEY(ds,iotId,eventName,optId)
) ENGINE=INNODB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT '飞燕设备日志日统计表';


./flink run -m yarn-cluster -ynm DWS_DEVICE_FEIYAN_LOG_EVENT -p 3 -ys 3 -yjm 1024 -ytm 2000m -d -c com.iotmars.compass.DeviceLogApp -yqu default /opt/jar/DWS_DEVICE_FEIYAN_LOG_EVENT-1.0-SNAPSHOT.jar