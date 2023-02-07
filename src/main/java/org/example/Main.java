package org.example;
import com.mongodb.spark.MongoSpark;
//import com.mongodb.spark.sql.connector.config.WriteConfig;
//import jersey.repackaged.com.google.common.collect.Multiset;
import com.mongodb.spark.config.WriteConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.bson.Document;
import scala.Char;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.types.DataTypes.StringType;

//import java.lang.reflect.Array;
//import java.util.*;
//import java.util.stream.Collectors;
//
//import org.apache.spark.sql.sources.In;
//import org.example.dataModel;
//
//import javax.swing.text.Document;

public class Main {
    public static void main(String[] args) {
try{
//        SparkSession sparkSession = SparkSession.builder().master("local").appName("ReadCsv").getOrCreate();
//        String filePath = ReadCsv.class.getResource("csvFile.csv").getPath();
//        Dataset<Row> data = sparkSession.sqlContext().read().format("com.databricks.spark.csv").load(filePath);
//        data.show();
//        System.out.println("Hello world!");




//Completion Feature in Data Quality++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//
//        SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").config("spark.driver.host", "localhost").getOrCreate();
//        Dataset<Row> csv1 = spark.read().csv("R:/Spring Projects/untitled1/src/main/resources/csvFile1.csv");
//
//        int count=0;
//        List<Integer> comList = null; //BlankRow
//        List<Row> list = csv1.collectAsList();
//        Integer colCount=list.get(0).size();
//        Integer BlankRow[]=new Integer[colCount+1];
//
//        for(int i=0;i<BlankRow.length;i++){
//            BlankRow[i]=0;
//        }
//
//
//        for(Row ele:list){
////            System.out.println(ele);
////            Integer curColBlankcell=0;
//            ++count;
//            for(int i=0;i<ele.length();i++){
//                try{
//                    ele.get(i).equals("");
////                    System.out.println();
//                }catch(java.lang.NullPointerException ex){
//                    System.out.println("On "+count+" Row and on "+(i+1)+" Column there is Empty Cell.");
////                    comList.add(i+1,++curColBlankcell);
//                    BlankRow[i+1]=BlankRow[i+1]+1;
//                }
//            }
//        }
////        System.out.println(BlankRow);
//    for(int i=1;i<BlankRow.length;i++){
////        System.out.println(BlankRow[i]);
////        System.out.println(BlankRow[i]);
////        System.out.println(list.size());
////        System.out.println(new Float(BlankRow[i]*100/(list.size()-1)));
//        System.out.println("+=============================================================================+");
//        System.out.println(list.get(0).get(i-1)+" Column has total "+BlankRow[i]+ " Blank cells");
//        System.out.println( list.get(0).get(i-1)+" Column has "+ (100-new Float(BlankRow[i]*100/(list.size()-1)))+"% of Completion." );
//    }
//



//Is unique Feature of Data Quality+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++



//    SparkSession spark1 = SparkSession.builder().appName("Simple Application").master("local").config("spark.driver.host", "localhost").getOrCreate();
//    Dataset<Row> csv0 = spark1.read().csv("R:/Spring Projects/untitled1/src/main/resources/csvFile1.csv");
//    List<Row> list1 = csv0.collectAsList();
//    HashMap hm= new HashMap();
//    String columnName="Name";
//    Integer columnNo=null;
//    for(int i=0;i<list1.get(0).length();i++){
////        System.out.println(list1.get(0).get(i));
//        if(list1.get(0).get(i).equals(columnName)){
//            columnNo=i;
//            break;
//        }
//    }
////    System.out.println(columnNo);
//    if(columnNo==null){
//        System.out.println("No such column Present");
//    }else{
//        Boolean unique=true;
//        for(int i=1;i<list1.size();i++){
//            Row ele=list1.get(i);
////            System.out.println(ele);
//            for(int j=0;j< ele.size();j++){
//                if(j==columnNo){
////                    System.out.println(ele.get(j));
//                    if(hm.get(ele.get(j))==null){
//                        hm.put(ele.get(j),1);
//                    }else{
//                        if(hm.get(ele.get(j)).equals(null)){
//                            continue;
//                        }
//                        unique=false;
//                        System.out.println(hm.get(ele.get(j)));
//                        hm.put(ele.get(j),(int) hm.get(ele.get(j))+1);
//                        System.out.println(ele.get(j)+" value in Column "+columnName+" is not Unique");
//                    }
//                }
//            }
//        }
//    System.out.println(hm);
//        if(unique==true){
//            System.out.println(columnName+" is Unique Column.");
//        }else{
//            System.out.println(columnName+" is not Unique Column.");
//        }
//    }

//    Connecting to MongoDB+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

//    SparkSession spark = SparkSession.builder()
//                    .master("local")
//                    .appName("MongoDBConnection")
//                    .config("spark.driver.host", "localhost")
//                    .config("spark.mongodb.input.uri", "mongodb+srv://RahulSinghDhek:IQpy326QQQKAkK2J@cluster0.dxzlfnc.mongodb.net/rahul21-DB.AccessUsers")
//                    .config("spark.mongodb.output.uri", "mongodb+srv://RahulSinghDhek:IQpy326QQQKAkK2J@cluster0.dxzlfnc.mongodb.net/rahul21-DB.AccessUsers")
//                    .getOrCreate();
//
//            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
////            JavaMongoR<Document> rdd = MongoSpark.load(jsc);
//            Dataset<Row> df = MongoSpark.load(jsc).toDF();
//
//                List<Document> documents = new ArrayList<>();
//            documents.add(new Document("userName", "POPO").append("password","k1244nadnfknsak").append("role", "ADMIN"));
//            documents.add(new Document("userName", "YOYO").append("password","sdjsajf828328").append("role", "ADMIN"));
//            documents.add(new Document("userName", "UEID").append("password","abdfhwfiws12344").append("role","ADMIN"));
////            documents.add(new Document("userName", "KSAUL").append("password","abdfhwfiws12344").append("role","ADMIN"));
////            documents.add(new Document("userName", "PWEBH").append("password","abdfhwfiws12344").append("role","ADMIN"));
//
//
//            WriteConfig writeConfig = WriteConfig.create(jsc);
//
//            MongoSpark.save(jsc.parallelize(documents), writeConfig);
//            df.show();
//
//            jsc.close();

//Adding Column in csv file++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    SparkSession spark1 = SparkSession.builder().appName("Simple Application").master("local").config("spark.driver.host", "localhost").getOrCreate();
    Dataset<Row> csv0 = spark1.read().option("header",true).csv("R:/Spring Projects/untitled1/src/main/resources/csvFile1.csv");
    char[] gender={'M','F'};
//    int r = (int) (Math.random() * (1 - 0)) + 0;
//    System.out.println(r);
//    csv0 = csv0.withColumn("pay",functions.abs(functions.rand()));
    csv0 = csv0.withColumn("Gender",lit(gender[(int) (Math.random() * (1 - 0)) + 0]));
    csv0.show();


//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//    csv1 = csv1.withColumn("Name", functions.when(csv1.col("Name").equalTo("RSD"),"Polio").otherwise(csv1.col("Name")));
//        csv1.show();
//        csv1.write().csv("R:\\Spring Projects\\untitled1\\src\\main\\resources\\csvNewFile");


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

class completness{
    private int columnNo;
    private int BlankRows;
    private int PercentageBlank;

    public completness(int columnNo, int blankRows, int percentageBlank) {
        this.columnNo = columnNo;
        BlankRows = blankRows;
        PercentageBlank = percentageBlank;
    }
    public completness(){
        this.BlankRows=0;
    }

    public int getColumnNo() {
        return columnNo;
    }

    public void setColumnNo(int columnNo) {
        this.columnNo = columnNo;
    }

    public int getBlankRows() {
        return BlankRows;
    }

    public void setBlankRows(int blankRows) {
        BlankRows = blankRows;
    }

    public int getPercentageBlank() {
        return PercentageBlank;
    }

    public void setPercentageBlank(int percentageBlank) {
        PercentageBlank = percentageBlank;
    }
}
