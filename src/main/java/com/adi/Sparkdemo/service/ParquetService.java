package com.adi.Sparkdemo.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ParquetService {

    @Autowired
    private SparkSession sparkSession;

    public String readAndJoinParquetFiles(String joinColumn) {
        // Load Parquet files as DataFrames
        Dataset<Row> df1 = sparkSession.read().parquet("/src/resources/parquet/file.parquet");
        Dataset<Row> df2 = sparkSession.read().parquet("/src/resources/parquet/file2.parquet");

        // Register DataFrames as temporary views
        df1.createOrReplaceTempView("table1");
        df2.createOrReplaceTempView("table2");

        // Perform SQL join operation
        String sqlQuery = String.format("SELECT * FROM table1 JOIN table2 ON table1.%s = table2.%s", joinColumn, joinColumn);
        Dataset<Row> sqlResult = sparkSession.sql(sqlQuery);

        // Show the result
        // sqlResult.show();
        String jsonResult = sqlResult.toJSON().collectAsList().toString();
        return jsonResult;
    }
}
