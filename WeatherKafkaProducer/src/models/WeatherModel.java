package models;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class WeatherModel {
	public double getTemp() {
		return temp;
	}
	public void setTemp(double temp) {
		this.temp = temp;
	}
	public double getFeels_like() {
		return feels_like;
	}
	public void setFeels_like(double feels_like) {
		this.feels_like = feels_like;
	}
	public double getTemp_min() {
		return temp_min;
	}
	public void setTemp_min(double temp_min) {
		this.temp_min = temp_min;
	}
	public double getTemp_max() {
		return temp_max;
	}
	public void setTemp_max(double temp_max) {
		this.temp_max = temp_max;
	}
	public double getPressure() {
		return pressure;
	}
	public void setPressure(double pressure) {
		this.pressure = pressure;
	}
	public double getHumidity() {
		return humidity;
	}
	public void setHumidity(double humidity) {
		this.humidity = humidity;
	}
	public double getSea_level() {
		return sea_level;
	}
	public void setSea_level(double sea_level) {
		this.sea_level = sea_level;
	}
	public double getGrnd_level() {
		return grnd_level;
	}
	public void setGrnd_level(double grnd_level) {
		this.grnd_level = grnd_level;
	}	
	public WeatherCoordModel getCoord() {
		return coord;
	}
	public void setCoord(WeatherCoordModel coord) {
		this.coord = coord;
	}
	public String getName() {
		return name;
	}
	public void setLat(String name) {
		this.name = name;
	}
	public LocalDateTime getDate() {
		return date;
	}
	public void setLat(LocalDateTime date) {
		this.date = date;
	}
	@Override
	public String toString() {
		return name + "," + temp
				+ "," + feels_like + "," + temp_min
				+ "," + temp_max + "," + pressure
				+ "," + humidity + "," + sea_level
				+ "," + grnd_level + "," + coord.getLat()+","+coord.getLon() + ","+date;
	}
	public WeatherModel(String name,double temp, double feels_like, double temp_min,
			double temp_max, double pressure, double humidity,
			double sea_level, double grnd_level,WeatherCoordModel coord,LocalDateTime date) {
		super();
		this.name = name;
		this.temp = temp;
		this.feels_like = feels_like;
		this.temp_min = temp_min;
		this.temp_max = temp_max;
		this.pressure = pressure;
		this.humidity = humidity;
		this.sea_level = sea_level;
		this.grnd_level = grnd_level;
		this.coord = coord;		
		this.date =date;
	}
	String name;
	double temp,feels_like,temp_min,temp_max,pressure,humidity,sea_level,grnd_level;
	WeatherCoordModel coord;
	LocalDateTime date;
	}
