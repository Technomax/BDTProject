import models.WeatherCoordModel;
import models.WeatherMainModel;
import models.WeatherModel;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import sender.Sender;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class WeatherKafkaProducer {
	static final String APP_ID = "f03bdb7bce99b2b7956999c7a168e7a4";

	public static void main(String[] args) throws Exception {
		HttpClient httpClient = HttpClients
				.custom()
				.setDefaultRequestConfig(
						RequestConfig.custom()
								.setCookieSpec(CookieSpecs.STANDARD).build())
				.build();
		List<Integer> zipCodeOfIA=new ArrayList();
		zipCodeOfIA.add(50601);
		zipCodeOfIA.add(50001);
		zipCodeOfIA.add(50002);
		zipCodeOfIA.add(50003);
		zipCodeOfIA.add(50830);
		zipCodeOfIA.add(52530);
		zipCodeOfIA.add(52201);
		zipCodeOfIA.add(51001);
		zipCodeOfIA.add(50510);
		zipCodeOfIA.add(52531);
		zipCodeOfIA.add(50005);
		zipCodeOfIA.add(52202);
		zipCodeOfIA.add(50006);
		zipCodeOfIA.add(50420);
		zipCodeOfIA.add(50511);
		zipCodeOfIA.add(50007);
		zipCodeOfIA.add(50008);
		zipCodeOfIA.add(50602);
		zipCodeOfIA.add(51002);
		zipCodeOfIA.add(50603);
		zipCodeOfIA.add(52556);
		zipCodeOfIA.add(52557);

		StringBuilder builder=new StringBuilder();
		for(int str:zipCodeOfIA){
			builder.append(WeatherKafkaProducer.getWeather(httpClient,str));
			builder.append("\n");
		}
		UUID uuid= UUID.randomUUID();
		Sender.sendWeather(uuid.toString(), builder.toString());
	}
	
	public static String getWeather(HttpClient httpClient, int zipId) throws Exception{

		URIBuilder uriBuilder = new URIBuilder(
				"https://api.openweathermap.org/data/2.5/weather?zip="+zipId+",us&appid="+APP_ID);
		HttpGet httpGet = new HttpGet(uriBuilder.build());
		HttpResponse response = httpClient.execute(httpGet);
		HttpEntity entity = response.getEntity();
		String responseBody = EntityUtils.toString(response.getEntity());
		//get the response json in the proper format
		Gson gson = new Gson();
		Map<String, Object> data = gson.fromJson(responseBody, Map.class);
		JsonObject jsonTree = (JsonObject) gson.toJsonTree(data);
		String mainJson = jsonTree.get("main").toString();
		WeatherMainModel mainModel = gson.fromJson(jsonTree.get("main")
				.toString(), WeatherMainModel.class);
		WeatherCoordModel coordModel = gson.fromJson(jsonTree.get("coord")
				.toString(), WeatherCoordModel.class);
		String city = gson.fromJson(jsonTree.get("name").toString(),
				String.class);
		LocalDateTime localDate = LocalDateTime.now();
		WeatherModel weatherModel = new WeatherModel(city, mainModel.getTemp(),
				mainModel.getFeels_like(), mainModel.getTemp_min(),
				mainModel.getTemp_max(), mainModel.getPressure(),
				mainModel.getHumidity(), mainModel.getSea_level(),
				mainModel.getGrnd_level(), coordModel, localDate);
		return weatherModel.toString();
	}
}
