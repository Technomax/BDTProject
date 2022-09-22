package cs523.SparkConsumer;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;

public class HBaseDBManager {
	private Configuration hbaseConfig;
	long rowCount=20;
	public HBaseDBManager() throws IOException
	{
		this.hbaseConfig = HBaseConfiguration.create();
		this.CreateTable();
		System.out.println("Table Created");
	}

	private void CreateTable() throws IOException
	{
		try (Connection connection = ConnectionFactory.createConnection(this.hbaseConfig);
				Admin admin = connection.getAdmin())
		{
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf("Weather"));
			table.addFamily(new HColumnDescriptor("Date"));
			table.addFamily(new HColumnDescriptor("City"));
			table.addFamily(new HColumnDescriptor("Temperature"));
			table.addFamily(new HColumnDescriptor("Pressure"));
			
			if (admin.tableExists(table.getTableName()))
			{
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
				admin.createTable(table);
				Table tbl = connection.getTable(TableName.valueOf("Weather"));		
				tbl.close();
			}
	}

//  city,temp,feels_like,temp_min,temp_max,pressure, humidity,sea_level,grnd_level,CoordLat,coordLon,Date;
//	Fairfield,296.17,297.14,295.07,296.81,1015.0,100.0,0.0,0.0,41.0039,-91.9576,2022-09-19T21:23:07.414
//	Fairfield,296.2,297.17,295.11,298.33,1015.0,100.0,0.0,0.0,41.0166,-91.9682,2022-09-19T21:23:07.695
//	Burlington,296.1,296.77,294.4,297.24,1015.0,89.0,0.0,0.0,40.8087,-91.117,2022-09-19T21:23:07.735
//	West Burlington,296.12,296.77,294.39,297.23,1015.0,88.0,0.0,0.0,40.8321,-91.1799,2022-09-19T21:23:07.781
	public void Write(String key,JavaRDD<String> rdd) throws IOException
	{
		System.out.println(rdd.first());
		try (Connection connection = ConnectionFactory.createConnection(this.hbaseConfig);
				Admin admin = connection.getAdmin())
		{
			Table hTable = connection.getTable(TableName.valueOf("Weather"));
			
			
			for(String weather:rdd.collect())
			{
				rowCount++;
				System.out.println(weather);
				String[] str=weather.split("\\,");
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
				Put p=addRow(String.valueOf(rowCount),str[0],str[1],str[2],str[3],
						str[4],str[5],str[6],str[7],str[8]
						,str[9],str[10],LocalDateTime.parse("1999-12-31 23:59", formatter));	
				hTable.put(p);
			}
			hTable.close();
		}
	}
	
	public  Put addRow(String row,String city, String temp, String feels_like, String temp_min, String temp_max,String pressure, String humidity, 
			String sea_level, String grnd_level,
			String CoordLat,String coordLon,LocalDateTime  date){
		Put p=new Put(Bytes.toBytes(row));
		p.add(Bytes.toBytes("Date"),Bytes.toBytes("Year"),Bytes.toBytes(String.valueOf(date.getYear())));
		p.add(Bytes.toBytes("Date"),Bytes.toBytes("Month"),Bytes.toBytes(String.valueOf(date.getMonthValue())));
		p.add(Bytes.toBytes("Date"),Bytes.toBytes("Day"),Bytes.toBytes(String.valueOf(date.getDayOfMonth())));
		p.add(Bytes.toBytes("Date"),Bytes.toBytes("Hour"),Bytes.toBytes(String.valueOf(date.getHour())));
		p.add(Bytes.toBytes("Date"),Bytes.toBytes("Minute"),Bytes.toBytes(String.valueOf(date.getMinute())));
		p.add(Bytes.toBytes("City"),Bytes.toBytes("Name"),Bytes.toBytes(city));
		p.add(Bytes.toBytes("City"),Bytes.toBytes("Lat"),Bytes.toBytes(CoordLat));
		p.add(Bytes.toBytes("City"),Bytes.toBytes("Lon"),Bytes.toBytes(coordLon));
		p.add(Bytes.toBytes("Temperature"),Bytes.toBytes("temp"),Bytes.toBytes(temp));
		p.add(Bytes.toBytes("Temperature"),Bytes.toBytes("feels_like"),Bytes.toBytes(feels_like));
		p.add(Bytes.toBytes("Temperature"),Bytes.toBytes("temp_min"),Bytes.toBytes(temp_min));
		p.add(Bytes.toBytes("Temperature"),Bytes.toBytes("c"),Bytes.toBytes(temp_max));
		p.add(Bytes.toBytes("Pressure"),Bytes.toBytes("pressure"),Bytes.toBytes(pressure));
		p.add(Bytes.toBytes("Pressure"),Bytes.toBytes("humidity"),Bytes.toBytes(humidity));
		p.add(Bytes.toBytes("Pressure"),Bytes.toBytes("sea_level"),Bytes.toBytes(sea_level));
		p.add(Bytes.toBytes("Pressure"),Bytes.toBytes("grnd_level"),Bytes.toBytes(grnd_level));
		return p;			
	}
}
