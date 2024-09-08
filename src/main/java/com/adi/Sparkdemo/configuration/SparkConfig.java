package com.adi.Sparkdemo.configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Value("${spark.master}")
    private String master;

    @Value("${spark.app.name}")
    private String appName;

    @Value("${spark.executor.memory}")
    private String executorMemory;

    @Value("${spark.driver.memory}")
    private String driverMemory;

    @Value("${spark.executor.cores}")
    private String executorCores;

    @Value("${spark.sql.shuffle.partitions}")
    private String shufflePartitions;

    @Value("${spark.sql.parquet.compression.codec}")
    private String parquetCompressionCodec;

    @Value("${spark.sql.parquet.filterPushdown}")
    private String parquetFilterPushdown;

    @Value("${spark.sql.autoBroadcastJoinThreshold}")
    private String autoBroadcastJoinThreshold;

    @Value("${spark.serializer}")
    private String serializer;

    @Value("${spark.kryo.registrationRequired}")
    private String kryoRegistrationRequired;

    @Value("${spark.dynamicAllocation.enabled}")
    private String dynamicAllocationEnabled;

    @Value("${spark.dynamicAllocation.minExecutors}")
    private String dynamicAllocationMinExecutors;

    @Value("${spark.dynamicAllocation.maxExecutors}")
    private String dynamicAllocationMaxExecutors;

    @Bean
    public SparkSession sparkSession() {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster(master)
                .set("spark.executor.memory", executorMemory)
                .set("spark.driver.memory", driverMemory)
                .set("spark.executor.cores", executorCores)
                .set("spark.sql.shuffle.partitions", shufflePartitions)
                .set("spark.sql.parquet.compression.codec", parquetCompressionCodec)
                .set("spark.sql.parquet.filterPushdown", parquetFilterPushdown)
                .set("spark.sql.autoBroadcastJoinThreshold", autoBroadcastJoinThreshold)
                .set("spark.serializer", serializer)
                .set("spark.kryo.registrationRequired", kryoRegistrationRequired)
                .set("spark.dynamicAllocation.enabled", dynamicAllocationEnabled)
                .set("spark.dynamicAllocation.minExecutors", dynamicAllocationMinExecutors)
                .set("spark.dynamicAllocation.maxExecutors", dynamicAllocationMaxExecutors);

        return SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }
}
