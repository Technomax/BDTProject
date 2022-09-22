package cs523.SparkConsumer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;

public class WeatherSerializable implements Serializable {
	private static final long serialVersionUID = 1L;
	public List<Tuple2<String,String>> keyword;
	public WeatherSerializable(){
		this.keyword=new ArrayList<Tuple2<String,String>>();
	}
	
	public String GetWeather()
	{
		if(this.keyword.isEmpty())
			return "";
		
		System.out.println("keyword"+this.keyword);
		return this.keyword.stream().map(t -> t._1()).collect(Collectors.joining(","));
	}
}
