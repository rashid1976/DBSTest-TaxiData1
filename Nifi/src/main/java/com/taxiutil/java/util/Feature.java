package com.td.java.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.HashMap;
import java.util.Map;
import org.geojson.GeoJsonObject;
import org.geojson.GeoJsonObjectVisitor;

public class Feature extends GeoJsonObject {


	private static final long serialVersionUID = -2660731192749082479L;
	@JsonInclude(JsonInclude.Include.ALWAYS)
	private Map<String, String> properties = new HashMap<String, String>();
	@JsonInclude(JsonInclude.Include.ALWAYS)
	private RichGeometry geometry;
	private String id;

	public void setProperty(String key, String value) {
		properties.put(key, value);
	}

	@SuppressWarnings("unchecked")
	public <T> T getProperty(String key) {
		return (T)properties.get(key);
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

	public RichGeometry getGeometry() {
		return geometry;
	}

	public void setGeometry(RichGeometry geometry) {
		this.geometry = geometry;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}



	@Override public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		if (!super.equals(o))
			return false;
		Feature feature = (Feature)o;
		if (properties != null ? !properties.equals(feature.properties) : feature.properties != null)
			return false;
		if (geometry != null ? !geometry.equals(feature.geometry) : feature.geometry != null)
			return false;
		return !(id != null ? !id.equals(feature.id) : feature.id != null);
	}

	@Override public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (properties != null ? properties.hashCode() : 0);
		result = 31 * result + (geometry != null ? geometry.hashCode() : 0);
		result = 31 * result + (id != null ? id.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "Feature{properties=" + properties + ", geometry=" + geometry + ", id='" + id + "'}";
	}

	@Override
	public <T> T accept(GeoJsonObjectVisitor<T> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

}