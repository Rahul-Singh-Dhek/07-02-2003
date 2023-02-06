package org.example;
import org.apache.spark.sql.*;

import java.util.Arrays;
import org.example.dataModel;

public class Main {
    public static void main(String[] args) {
try{
//        SparkSession sparkSession = SparkSession.builder().master("local").appName("ReadCsv").getOrCreate();
//        String filePath = ReadCsv.class.getResource("csvFile.csv").getPath();
//        Dataset<Row> data = sparkSession.sqlContext().read().format("com.databricks.spark.csv").load(filePath);
//        data.show();
//        System.out.println("Hello world!");
        SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").config("spark.driver.host", "localhost").getOrCreate();
        Dataset<Row> csv1 = spark.read().option("header",true).csv("R:/Spring Projects/untitled1/src/main/resources/csvFile1.csv");
        csv1.show();
    csv1 = csv1.withColumn("Name", functions.when(csv1.col("Name").equalTo("RSD"),"Polio").otherwise(csv1.col("Name")));
        csv1.show();
        csv1.write().csv("R:\\Spring Projects\\untitled1\\src\\main\\resources\\csvNewFile");


//        csv1.write().csv("R:\\Spring Projects\\untitled1\\src\\main\\resources");

    //Writing a csv file:-------------------------------------------------------------------------------------

//        Dataset<Row> data = spark.createDataFrame(Arrays.asList(
//                    new dataModel("RSD","8354973790","j"),
//                    new dataModel("LSD","6393578389","j"),
//                    new dataModel("POP","7318312301","z"),
//                new dataModel("Vishal","823832898","v")
//            ), dataModel.class);
//        data.show();
//        data.write().format("csv").option("header",true).mode(SaveMode.Overwrite).save("R:/Spring Projects/untitled1/src/main/resources/csv");

//        Appending one CSV file into another File:------------------------------------------------------------------

//        Dataset<Row> df=spark.createDataFrame(Arrays.asList(
//                new dataModel1("Polio"),
//                new dataModel1("Covid")
//        ),dataModel1.class);
//        df.show();
//
//    df.union(spark.read().csv("R:\\Spring Projects\\untitled1\\src\\main\\resources\\csvFile1.csv"))
//            .coalesce(1)
//            .write()
//            .format("csv")
//            .mode("append");
////            .save("R:\\Spring Projects\\untitled1\\src\\main\\resources\\AppendCsv");
//
//    df.show();

}catch(Exception ex){
    System.out.println(ex);
}
    }
}
