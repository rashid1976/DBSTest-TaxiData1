package com.td.java.util;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.SpatialReference;



public class RichGeometry {
	
	 Geometry geometry;
	 SpatialReference   spatialReference  = SpatialReference.create(4326); 
		    
	 public RichGeometry(Geometry g){
		 this.geometry = g;
	 }
	 public boolean  contains( Geometry other){
			return  GeometryEngine.contains(this.geometry, other, spatialReference);
	  }
}
