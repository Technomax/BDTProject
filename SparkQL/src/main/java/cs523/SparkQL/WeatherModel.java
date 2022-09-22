package cs523.SparkQL;

import java.io.Serializable;

public class WeatherModel implements Serializable {
	public String getTemp_min() {
		return temp_min;
	}

	public void setTemp_min(String temp_min) {
		this.temp_min = temp_min;
	}

	public String getTemp_max() {
		return temp_max;
	}

	public void setTemp_max(String temp_max) {
		this.temp_max = temp_max;
	}

	public String getName() {
		return name;
	}

	public void setLat(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return name + "," + temp_min + "," + temp_max + "," + day + "," + month
				+ "," + year;
	}

	String name;
	String temp_min, temp_max;
	String month, year, day;

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}

	public void setName(String name) {
		this.name = name;
	}
}
