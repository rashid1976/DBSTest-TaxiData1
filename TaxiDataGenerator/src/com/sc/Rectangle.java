package com.sc;

public class Rectangle {
	Point top;
	Point east;
	Point southEast;
	Point south;
	String id;
	int row;
	int column;
	
	
	@Override
	public String toString() {
		return "{ \"type\": \"Feature\", \"id\":    \""+getId()+" \", \"properties\": { \"boroughCode\": 1.1, \"borough\": \"Island\", \"@"+getId()+"\": \"\" }, \"geometry\": { \"type\": \"Polygon\", \"coordinates\": [ ["+
				top.getCoordinate() +","+ east.getCoordinate() +","+ southEast.getCoordinate() +","+ south.getCoordinate() +","+top.getCoordinate() 
				+" ] ] } }";
	}
	
	
	
	
	public int getRow() {
		return row;
	}

	public void setRow(int row) {
		this.row = row;
	}

	public int getColumn() {
		return column;
	}

	public void setColumn(int column) {
		this.column = column;
	}




	public String getId() {
		return row+"."+column;
	}


	public void setId(String id) {
		this.id = id;
	}


	public Point getTop() {
		return top;
	}
	public void setTop(Point top) {
		this.top = top;
	}
	public Point getEast() {
		return east;
	}
	public void setEast(Point east) {
		this.east = east;
	}
	public Point getSouthEast() {
		return southEast;
	}
	public void setSouthEast(Point southEast) {
		this.southEast = southEast;
	}
	public Point getSouth() {
		return south;
	}
	public void setSouth(Point south) {
		this.south = south;
	}
	
	

}
