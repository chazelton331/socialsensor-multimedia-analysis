package eu.socialsensor.multimedia.indexing;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class StormVisualIndexer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
//		String mongoHost = args[0];
//		String mongoDbName = args[1]; 
//		String mongoCollectionName = args[2];
//		
//		String indexHostname = args[3];
//		String indexColection = args[4];
//		String codebook = args[5]; 
//		String pcaFile = args[6];
		
		String mongoHost = "160.40.50.207";
		String mongoDbName = "Streams"; 
		String mongoCollectionName = "MediaItemsFromWP_boilerpipe";
		
		String indexHostname = "http://160.40.50.207:8080/VisualIndex";
		String indexColection = "prototype";
		
		String learningFolder = "/home/manosetro/git/multimedia-indexing/learning_files/";
		
		String[] codebookFiles = { 
				learningFolder + "surf_l2_128c_0.csv",
				learningFolder + "surf_l2_128c_1.csv", 
				learningFolder + "surf_l2_128c_2.csv",
				learningFolder + "surf_l2_128c_3.csv" };
		
		String pcaFile = learningFolder + "pca_surf_4x128_32768to1024.txt";
		
		
		DBObject query = new BasicDBObject("status", "new");
		query.put("type", "image");
	
		VisualIndexerBolt visualIndexer;
		try {
			visualIndexer = new VisualIndexerBolt(indexHostname, indexColection, codebookFiles, pcaFile);
		} catch (Exception e) {
			return;
		}
		
		UpdaterBolt updater = new UpdaterBolt(mongoHost, mongoDbName, mongoCollectionName);
		
		TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("injector", new MongoDBInjector(mongoHost, mongoDbName, mongoCollectionName, query), 1);
        builder.setBolt("ranker", new MediaRankerBolt(), 2).shuffleGrouping("injector");
        builder.setBolt("indexer", visualIndexer, 2).shuffleGrouping("ranker");
     
		builder.setBolt("updater", updater, 2).shuffleGrouping("indexer");
        
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("visual-indexer", conf, builder.createTopology());
        
	}

}
