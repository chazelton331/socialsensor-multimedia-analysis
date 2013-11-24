package eu.socialsensor.framework.multimedia.spatial.clustering;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.socialsensor.framework.multimedia.spatial.geometry.Point;


public class ItemContainer {

	private String id;
	private String userID;
	private Point geo;
	private long timeCreated;
	private String text;
	
	//private String timeCreatedHumanReadable;
	
	private List<Integer> clusterAssignments;
	
	public ItemContainer(String id) {
		this.id = id;
	}
	
	public ItemContainer(String id, String userID, double latitude, double longitude, 
			long timeCaptured, String text) {
		this.id = id;
		this.userID = userID;
		this.geo = new Point(latitude, longitude);
		this.timeCreated = timeCaptured;
		this.text = text;
		//this.timeCreatedHumanReadable = timeCreatedHumanReadable;
	}
	
	public ItemContainer(String id, String userID, double latitude, double longitude, 
			List<Integer> clusterAssignments){
		this.id = id;
		this.userID = userID;
		this.geo = new Point(latitude, longitude);
		//this.timeCreatedHumanReadable = timeCreatedHumanReadable;
		this.clusterAssignments = clusterAssignments;
	}
	
	public ItemContainer(String id, long timeCaptured){
		this.id = id;
		this.timeCreated = timeCaptured;
	}

	public String getId() {
		return id;
	}

	public double getLatitude() {
		return geo.getLatitude();
	}

	public double getLongitude() {
		return geo.getLongitude();
	}

	public long getTimeCreated() {
		return timeCreated;
	}

	public String getText() {
		return text;
	}
	
//	public String getTimeCreatedHumanReadable() {
//		return timeCreatedHumanReadable;
//	}

	public String getUserID() {
		return userID;
	}

	public List<Integer> getClusterAssignments() {
		return clusterAssignments;
	}
	
	public String toString() {
		return id+"\t" + userID + "\t" + getLongitude() + "\t"+ getLongitude();// + "\t" + timeCreated;
	}
	
	public Map<String, Object> toMap() {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("id", this.id);
		map.put("coordinates", this.geo.toMap());
		try {
			String stream = this.id.split("::")[0];
			map.put("stream", stream);
		}
		catch(Exception e) {}
		
		return map;
	}
}
