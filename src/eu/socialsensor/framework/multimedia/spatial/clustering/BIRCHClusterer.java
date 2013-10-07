package eu.socialsensor.framework.multimedia.spatial.clustering;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;


import edu.gatech.gtisc.jbirch.cftree.CFTree;
import eu.socialsensor.framework.client.dao.MediaItemDAO;
import eu.socialsensor.framework.client.dao.impl.MediaItemDAOImpl;
import eu.socialsensor.framework.client.mongo.MongoHandler;
import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.framework.multimedia.spatial.geometry.ExtGrahamScan;
import eu.socialsensor.framework.multimedia.spatial.geometry.Point;

public class BIRCHClusterer extends TimerTask {
	
	private MediaItemDAO dao;
	private MongoHandler mongo;
	
	private SolrServer solr = null;

	public BIRCHClusterer(String host, String db, String collection, String solr_host) throws Exception {
		this.dao = new MediaItemDAOImpl(host, db, collection);
		try {
			this.mongo = new MongoHandler(host, db, "Hotspots", null);
		} catch (UnknownHostException e) {
			throw new Exception(e);
		}
		
		if(solr_host != null) {
			this.solr = new HttpSolrServer(solr_host);
		}
	}
	
	public static class BirchRun {
		public double distanceParameter;
		public int runIndex;
		
		public BirchRun(double distanceParameter, int runIndex) {
			this.distanceParameter = distanceParameter;
			this.runIndex = runIndex;
		}
	}
	
	public static Map<Integer, List<String>> birchClustering(BirchRun[] birchRunParameters, Map<String, ItemContainer> elements) {
			
		Map<Integer, List<Integer>> results = BIRCHClusterer.multipleClusterings(birchRunParameters, elements);
		Map<Integer, List<String>> clusters = getClusterResults(elements, results);
		clusters.remove(0);
		
		return clusters;
	}
	
	public static int NUMBER_OF_CLUSTERING_RUNS;

	public static Map<Integer, Integer> birchCluster(double distance, Map<String, ItemContainer> elements) {
		System.out.print("BIRCH clustering (distance " + distance + ") ... " );
		
		int maxNodeEntries = elements.size();
		boolean applyMergingRefinement = true;
		CFTree birchTree = new CFTree(maxNodeEntries,distance,CFTree.D1_DIST,applyMergingRefinement);
		birchTree.setMemoryLimit(1000 * 1024 * 1024);
		
		for(Entry<String, ItemContainer> entry : elements.entrySet()) {
			double[] x = new double[2];
			x[0] = entry.getValue().getLatitude();
			x[1] = entry.getValue().getLongitude();
			
			boolean inserted = birchTree.insertEntry(x);
			if(!inserted) {
				System.err.println("NOT INSERTED!");
				System.exit(1);
			}
		}
		
		CFTree oldTree = birchTree;
		CFTree newTree = null;
		double newThreshold = distance;
		for(int i=0; i<3; i++) {
			newThreshold = oldTree.computeNewThreshold(oldTree.getLeafListStart(), CFTree.D1_DIST, newThreshold);
			newTree = oldTree.rebuildTree(maxNodeEntries, newThreshold, CFTree.D1_DIST, true, false);
			oldTree = newTree;
		}
		ArrayList<ArrayList<Integer>> members = newTree.getSubclusterMembers();
		System.out.println("END");
		return createClusterAssignmentsMap(members);
	}
	
	public static Map<Integer, List<Integer>> multipleClusterings(BirchRun[] birchRunParamenters, Map<String, ItemContainer> elements){
		NUMBER_OF_CLUSTERING_RUNS = birchRunParamenters.length;
		
		Map<Integer, List<Integer>> results = new HashMap<Integer, List<Integer>>();
		
		for(int i = 0; i < birchRunParamenters.length; i++) {
			Map<Integer, Integer> tempClusterAssignments = birchCluster(birchRunParamenters[i].distanceParameter, elements);
			for(int j = 1; j <= tempClusterAssignments.size(); j++) {
				if( !results.containsKey(j) ) {
					List<Integer> assignments = new ArrayList<Integer>();
					assignments.add(tempClusterAssignments.get(j));
					results.put(j, assignments);
				}else{
					results.get(j).add(tempClusterAssignments.get(j));
				}
			}
		}
		
		return results;
	}
	
	@SuppressWarnings("unchecked")
	public static List<ItemContainer>[] getClusters(int numberOfClusteringRun, Map<String, ItemContainer> elements){
		int numberOfClusters = getNumberOfClusters(numberOfClusteringRun, elements);
		List<ItemContainer>[] results = (List<ItemContainer>[])new ArrayList[numberOfClusters];
		for(Entry<String, ItemContainer> entry : elements.entrySet()){
			int assignment = entry.getValue().getClusterAssignments().get(numberOfClusteringRun - 1);
			if(assignment == 0){
				continue;
			}
			if(results[assignment - 1] == null){
				List<ItemContainer> tempList = new ArrayList<ItemContainer>();
				tempList.add(entry.getValue());
				results[assignment - 1] = tempList;
			}else{
				results[assignment - 1].add(entry.getValue());
			}
		}
		return results;
	}

	private static int getNumberOfClusters(int numberOfClusteringRun, Map<String, ItemContainer> elements) {
		if( (numberOfClusteringRun <= 0) || (numberOfClusteringRun > NUMBER_OF_CLUSTERING_RUNS) ){
			throw new IllegalArgumentException("wrong number of clustering run. It must be 1 <= " + 
					numberOfClusteringRun + " <= " + NUMBER_OF_CLUSTERING_RUNS);
		}
		List<Integer> clusterAssignments = new ArrayList<Integer>();
		Collection<ItemContainer> values = elements.values();
		Iterator<ItemContainer> iter = values.iterator();
		while(iter.hasNext()){
			clusterAssignments.add(iter.next().getClusterAssignments().get(numberOfClusteringRun-1));
		}
		Collections.sort(clusterAssignments, Collections.reverseOrder());
		return clusterAssignments.get(0);
	}
	
	// Get results fot the first set of parameters
		public static Map<Integer, List<String>> getClusterResults (
				Map<String, ItemContainer> elements, Map<Integer, List<Integer>> results){
			int i = 0;
			HashMap<Integer, List<String>> clusters = new HashMap<Integer, List<String>>();
			for(String id : elements.keySet()) {
				ItemContainer item = elements.get(id);				
				List<Integer> assignments = results.get(++i);
				Integer assignment = assignments.get(0);		
				List<String> cluster = clusters.get(assignment);
				if(cluster == null) {
					cluster = new ArrayList<String>();
					clusters.put(assignment, cluster);
				}
				cluster.add(item.getId());
				
			}
			return clusters;
		}
		
		public static Point getMedianGeo(List<Point> points) {
			Point median = null;
	    	Double min_distance = Double.MAX_VALUE;
	    	for(Point p1 : points) {
	    		Double distance = 0.0;
	    		for(Point p2 : points) {
	    			distance += GeodesicDistanceCalculator.distVincenty(p1, p2);
	        	}
	    		if(distance < min_distance) {
	    			median = p1;
	    			min_distance = distance;
	    		}
	    	}
	    	return median;
	    }
		
		public static Map<Integer, Integer> createClusterAssignmentsMap(ArrayList<ArrayList<Integer>> members){
			Map<Integer, Integer> assignments = new HashMap<Integer, Integer>();
			for(int i = 0; i < members.size(); i++){
				if(members.get(i).size() == 1){
					assignments.put(members.get(i).get(0), 0);
				}else{
					for(int j = 0; j < members.get(i).size(); j++){
						assignments.put(members.get(i).get(j), i+1);
					}
				}
				
			}
			return assignments;
		}
		
	public static void main(String[] args) {
		
		Timer timer = new Timer(); 
		try {
			timer.schedule(new BIRCHClusterer(
					"160.40.50.207", "mmdemo", "MediaItems", 
					"http://160.40.50.207:8080/solr-4.2.1/Hotspots"), (long)100, (long)15*60000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void run() {
		
		int minNumberOfMembers = 4;
		
		long ts = System.currentTimeMillis();
		
		BirchRun[] birchRunParameters = {
				new BirchRun(0.0025, 1)
				};
		
		
		//birch clustering
		System.out.println("Start BIRCH clustering...");
		Map<String, ItemContainer> elements = new HashMap<String, ItemContainer>();
		
		List<MediaItem> mediaItems = dao.getLastMediaItemsWithGeo(80000);
		System.out.println(mediaItems.size());
		for(MediaItem mi : mediaItems) {
			ItemContainer item = new ItemContainer(
					mi.getId(), mi.getAuthor(), mi.getLatitude(), 
					mi.getLongitude(), mi.getPublicationTime(), mi.getTitle());
			
			elements.put(mi.getId(), item);
		}
		
		Map<Integer, List<String>> clusters = BIRCHClusterer.birchClustering(birchRunParameters, elements);
		
		System.out.println("Clusters found: " + clusters.size());

		//Map<String, Object> birch_results = new HashMap<String, Object>();
		int i = 0;
		//List<Map<?, ?>> hotspots = new ArrayList<Map<?, ?>>();
		for(Entry<Integer, List<String>> cluster : clusters.entrySet()) {
			int count = cluster.getValue().size();
			if(count>minNumberOfMembers) {
				
				Map<String, Object> map = new HashMap<String, Object>();
				
				List<String> ids = cluster.getValue();
				List<Map<String, Object>> members = new ArrayList<Map<String, Object>>();
				List<Point> points = new ArrayList<Point>();
				
				StringBuffer text = new StringBuffer();
				
				for(String member_id : ids) {
					ItemContainer item = elements.get(member_id);
					
					String item_text = item.getText();
					if(item_text != null) {
						text.append(item_text + " ");
					}
					
					
					double latitude = item.getLatitude();
					double longitude = item.getLongitude();
					
					points.add(new Point(latitude, longitude));
					
					members.add(item.toMap());
				}
	
				Point median = getMedianGeo(points);
				ExtGrahamScan scan = new ExtGrahamScan(points.toArray(new Point[points.size()]));
				List<Map<String, Double>> convexHull = scan.convex_hull();
				
				String hotspot_id = ts + "_" + (++i);
						
				map.put("_id", hotspot_id);
				map.put("timestamp", ts);
				map.put("items", members);
				map.put("items_count", count);
				map.put("median_geolocation", median.toMap());
				if(convexHull != null) {
					map.put("convex_hull", convexHull);
				}
				
				mongo.insert(map);
				
				if(solr != null && text.length()>0) {
					SolrInputDocument doc = new SolrInputDocument();
					doc.addField("id", hotspot_id);
					doc.addField("time", ts);
					doc.addField("text", text.toString());
					try {
						solr.add(doc);
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println("Exception in Solr: " + e.getMessage());
					} 
				}
				
			}
		}
		
		System.out.println("Remaining Clusters: " + i);
		
		try {
			solr.commit();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Exception in Solr commit=> " + e.getMessage());
		} 
	
	}

}
