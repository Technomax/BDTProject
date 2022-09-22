package cs523.SparkConsumer;

import java.io.InputStream;
import java.util.Properties;

public class WeatherResources {
	public String TOPIC;
	public String PORT;
	
	public WeatherResources() throws Exception
	{
		InputStream inputStream=null;
		try 
		{
			Properties prop = new Properties();
			//inputStream = getClass().getClassLoader().getResourceAsStream("/config.properties");
			inputStream = getClass().getResourceAsStream("config.properties");
			if (inputStream != null) 
			{
				prop.load(inputStream);
				this.TOPIC=prop.getProperty("TOPIC");
				this.PORT=prop.getProperty("PORT");
			}
		} 
		catch (Exception ex) 
		{ 
		} 
		finally 
		{
			if (inputStream != null)
				inputStream.close();
		}
	}

}
