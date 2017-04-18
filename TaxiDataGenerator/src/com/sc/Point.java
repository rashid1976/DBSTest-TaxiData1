package com.sc;

public class Point {
	
	public Point(double xx,double yy){
		x=xx;
		y=yy;
	}
	
	double x;
	double y;
	String coordinate;
	
	public double getX() {
		return x;
	}
	public void setX(double x) {
		this.x = x;
	}
	public double getY() {
		return y;
	}
	public void setY(double y) {
		this.y = y;
	}
	public String getCoordinate() {
		return "["+y+","+x+"]";
	}
	public void setCoordinate(String coordinate) {
		this.coordinate = coordinate;
	}
	
	
	

}
