package eu.socialsensor.framework.multimedia.spatial.geometry;

import java.util.HashMap;
import java.util.Map;


/**
 * Class that contains geo information (latitude, longitude) 
 * @author papadop
 *
 */
public class Point {
	
	private double longitude;
	private double latitude;

	public Point(double latitude, double longitude) {
		super();
		this.longitude = longitude;
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public double getLatitude() {
		return latitude;
	}

	@Override
	public String toString() {
		return "(" + longitude + ", " + latitude + ")";
	}
	
	public Map<String, Double> toMap() {
		Map<String, Double> map = new HashMap<String, Double>();
		
		map.put("latitude", latitude);
		map.put("longitude", longitude);
		
		return map;
	}
}
