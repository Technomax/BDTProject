package cs523.SparkConsumer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.network.protocol.Encoders;

import scala.Console;
import scala.Tuple2;

public class WeatherSpark {
	private JavaSparkContext sc;
	private HBaseDBManager db;
	private HashMap<String,String> mapKey;
	
	public WeatherSpark() throws IOException
	{
     	this.sc=new JavaSparkContext("local[*]","WeatherSpark",new SparkConf());
		this.db=new HBaseDBManager();
	}
	
	public void Process(String key, List<String> inputLst) throws IOException
	{
	    JavaRDD<String> list=sc.parallelize(inputLst);	   
		this.db.Write(key,list);
	}
}
