package com.sc;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class Coordinate {
	
	
	
	//41.474937, -74.913585 
	
	public static void main(String args[]) throws IOException{
		Point point = new Point(41.47718278821029,-74.91658260738);
		FileWriter fw = new FileWriter(new File("C:\\Users\\Rashid Ahmed Khan\\git\\TaxiDataGenerator\\src\\taxiData.geojson"));
		fw.write("{\"type\": \"FeatureCollection\",\"features\": [  \n");
		ArrayList<Rectangle> firstRow = new ArrayList<Rectangle>();
		
		
		
		for(int col=1; col <=300; col++){
			Rectangle rect =  createRectange(point);
			rect.setRow(1);
			rect.setColumn(col);
			firstRow.add(rect);
			point = rect.getEast();
		}
		
				
		
		
		for(int row=1; row <=300;row++  ){
			for(int col=0; col <300; col++){
				Rectangle rect =  createRectange(firstRow.get(col).getSouth());
				rect.setRow(row);
				rect.setColumn(col+1);
				fw.write(rect+", \n");
				firstRow.set(col,rect);
			}
		}
		
		fw.write("]}");
		fw.close();
		
	}
	
	
	
	public static Rectangle createRectange(Point basepoint){
		Rectangle rect = new Rectangle();
		rect.setTop(basepoint);
		rect.setEast(getNewCord(basepoint.getX(),basepoint.getY() ,0,500));
		rect.setSouthEast(getNewCord(basepoint.getX(),basepoint.getY() ,-500,500));
		rect.setSouth(getNewCord(basepoint.getX(),basepoint.getY() ,-500,0));
		return rect;
	}
	
	
	public static Point getNewCord(double lat, double lon, double dn, double de){
		//Position, decimal degrees
		 //lat = 51.0
		 //lon = 0.0

		 //Earth�s radius, sphere
		 double R=6378137 ;

		 //offsets in meters
		 //dn = 100
		 //de = 100

		 //Coordinate offsets in radians
		 double dLat = dn/R ;
		 double dLon = de/(R*Math.cos(Math.PI*lat/180));

		 //OffsetPosition, decimal degrees
		 double latO = lat + dLat * 180/Math.PI;
		 double lonO = lon + dLon * 180/Math.PI;
		 return new Point (latO,lonO);
	}
	
	
	
	
	
	
	

}
