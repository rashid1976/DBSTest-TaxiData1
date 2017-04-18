package com.td.java.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.json.JSONException;

import com.esri.core.geometry.GeoJsonImportFlags;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.OperatorImportFromGeoJson;
import com.esri.core.geometry.Point;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class GeoUtil {
	
	ArrayList<Feature> features = new ArrayList<Feature>();
	String cellId="";
	public static String file="";
	public static GeoUtil geo=null ;

	public static GeoUtil getInstance(){
      try {
		  if (geo == null) {
			  geo = new GeoUtil(file);
		  }
	  }catch(JSONException e){
      	e.printStackTrace();
	  }catch(IOException ie){
	  	ie.printStackTrace();
	  }
		return geo;
	}
	public static void setFilePath(String p){
		file = p;
	}
	
	
   public static void main(String args[]) throws JsonParseException, JsonMappingException, IOException, JSONException{
		GeoUtil geo = new GeoUtil("C:\\Users\\1529018\\workspace\\DisplayXML\\WebContent\\taxiData.geojson");
		//medallion,hack_license,vendor_id,rate_code,store_and_fwd_flag,pickup_datetime,dropoff_datetime,passenger_count,trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude
		BufferedReader  br = new BufferedReader(new FileReader("C:\\TaxiData\\trip_data\\test_data.csv"));
		String line ="";
		int count=0;
		
		while((line = br.readLine())!= null){
			String[] token = line.split(",");
            count++;
			double pickup_longitude = Double.valueOf(token[10]);
			double pickup_latitude = Double.valueOf(token[11]);
			double dropoff_longitude = Double.valueOf(token[12]);
			double dropoff_latitude = Double.valueOf(token[13]);
		
			if(geo.contain(   pickup_longitude,pickup_latitude)){
				System.out.println(count+" :Pick_up_location:"+geo.getCellId());
			}else{
				System.out.println("pick up is out:"+count);
			}
			
			if(geo.contain( dropoff_longitude ,dropoff_latitude )){
				System.out.println(count+" :drop_off_location:"+geo.getCellId());
			}
			else{
				System.out.println("dropp off is out:"+count);
			}
			
		}
		
		br.close();
		
	}

	
	public GeoUtil(String filePath) throws JsonParseException, JsonMappingException, IOException, JSONException{
		FileInputStream inputStream = new FileInputStream(new File(filePath));
		java.util.LinkedHashMap geoJsonMap =  new ObjectMapper().readValue(inputStream, java.util.LinkedHashMap.class);
		
		if("FeatureCollection".equalsIgnoreCase((String)geoJsonMap.get("type"))){
			java.util.ArrayList<java.util.LinkedHashMap> featureCollection = (ArrayList) geoJsonMap.get("features");
			for(java.util.LinkedHashMap feature: featureCollection){
				Feature f = new Feature();
				f.setId(feature.get("id").toString());
				f.setProperties((Map<String, String> )feature.get("properties"));
				String geoJsonString = new ObjectMapper().writeValueAsString(feature.get("geometry"));
				//System.out.println(geoJsonString);
			    MapGeometry g = OperatorImportFromGeoJson.local().execute(GeoJsonImportFlags.geoJsonImportDefaults, Geometry.Type.Polygon, geoJsonString, null);
				f.setGeometry(new RichGeometry(g.getGeometry()));
				features.add(f);
			 }
		}
		else{
			System.out.println("Invalie Geo Json format !!!");
		}
	}
	public String getCellId() {
		return cellId;
	}

	public void setCellId(String cellId) {
		this.cellId = cellId;
	}
	public boolean contain(double x, double y){
		boolean found = false;
		Point pt1 = new Point( x , y);
		 for(Feature feature: features){
			 if(feature.getGeometry().contains(pt1)){
				 found = true;
				 setCellId(feature.getId());
				 return found;
			 }
		 }
		return found; 
	}
	
	






}
