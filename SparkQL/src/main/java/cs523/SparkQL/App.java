package cs523.SparkQL;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.AnalysisException;


public class App {

public static void main(String[] args) throws AnalysisException, IOException {

	SparkConf conf= new SparkConf().setAppName("SparkQL").setMaster("local[*]");
	JavaSparkContext sc=new JavaSparkContext(conf);
 	SparkSession spark = SparkSession
      .builder()
      .appName("SparkQL1")
      .config(conf)
      .getOrCreate();
 	showWeatherAnalysis(sc,spark);
    spark.stop();
    sc.close();
  }
	
  private static void showWeatherAnalysis(JavaSparkContext sc,SparkSession spark) throws IOException {
    JavaRDD<WeatherModel> weatherRDD=sc.parallelize(new HBaseReader().GetWeatherData());
    List<StructField> fields = new ArrayList<>();
    	fields.add(DataTypes.createStructField("name", DataTypes.StringType, false));
    	fields.add(DataTypes.createStructField("year", DataTypes.StringType, false));
    	fields.add(DataTypes.createStructField("month", DataTypes.StringType, false));
    	fields.add(DataTypes.createStructField("day", DataTypes.StringType, false));
    	fields.add(DataTypes.createStructField("temp_min", DataTypes.StringType, false));
    	fields.add(DataTypes.createStructField("temp_max", DataTypes.StringType, true));
    	
    StructType schema = DataTypes.createStructType(fields);
    JavaRDD<Row> rowRDD = weatherRDD.map((Function<WeatherModel, Row>) record -> 
    {
    		return RowFactory.create(record.name , record.year,record.month,record.day,record.temp_min,record.temp_max);
    });
    Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, schema);
    dataFrame.createOrReplaceTempView("weathers");

    Dataset<Row> weatherResult = spark.sql("SELECT name,year,month,day,temp_min FROM weathers");
    weatherResult.show(100);

    Dataset<Row> weatherAvg = spark.sql("SELECT name,count(*),avg(temp_min) avgMin, min(temp_min) lowestMin, max(temp_min) highestMin FROM weathers group by name");
    weatherAvg.show(50);
    
    weatherResult.write().mode("append").option("header","true").csv("hdfs://localhost/user/cloudera/WeatherResult");
    weatherAvg.write().mode("append").option("header","true").csv("hdfs://localhost/user/cloudera/WeatherResult");
    
  }
}
