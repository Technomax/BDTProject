package models;

public class WeatherCoordModel {
	
	public double getLon() {
		return lon;
	}

	public void setLon(double lon) {
		this.lon = lon;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public WeatherCoordModel(double lon, double lat) {
		super();
		this.lon = lon;
		this.lat = lat;
	}

	double lon,lat;
	
	
}