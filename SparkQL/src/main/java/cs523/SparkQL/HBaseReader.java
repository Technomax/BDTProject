package cs523.SparkQL;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseReader {
	private Configuration hbaseConfig;

	public HBaseReader() {
		this.hbaseConfig = HBaseConfiguration.create();
	}

	public List<WeatherModel> GetWeatherData() throws IOException {
		List<WeatherModel> weatherList = new ArrayList<WeatherModel>();
		Connection connection = ConnectionFactory
				.createConnection(this.hbaseConfig);
			Table table = connection.getTable(TableName.valueOf("Weather"));
			Scan scan = new Scan();
			scan.setCacheBlocks(false);
			scan.setCaching(10000);
			scan.setMaxVersions(10);
			ResultScanner scanner = table.getScanner(scan);
			for (Result result = scanner.next(); result != null; result = scanner
					.next()) {
				WeatherModel weather = new WeatherModel();
				byte[] year = result.getValue(Bytes.toBytes("Date"),
						Bytes.toBytes("Year"));
				byte[] month = result.getValue(Bytes.toBytes("Date"),
						Bytes.toBytes("Month"));
				byte[] day = result.getValue(Bytes.toBytes("Date"),
						Bytes.toBytes("Day"));
				byte[] city = result.getValue(Bytes.toBytes("City"),
						Bytes.toBytes("Name"));
				byte[] min = result.getValue(Bytes.toBytes("Temperature"),
						Bytes.toBytes("temp_min"));
				byte[] max = result.getValue(Bytes.toBytes("Temperature"),
						Bytes.toBytes("temp_max"));
				weather.setName(Bytes.toString(city));
				weather.setYear(Bytes.toString(year));
				weather.setMonth(Bytes.toString(month));
				weather.setDay(Bytes.toString(day));
				weather.setTemp_min(Bytes.toString(min));
				weather.setTemp_max(Bytes.toString(max));
				weatherList.add(weather);
		}
		return weatherList;
	}
}
