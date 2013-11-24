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
		
		String mongoHost = "social1.atc.gr";
		String mongoDbName = "Streams"; 
		String mongoCollectionName = "MediaItems";
		
		String indexHostname = "http://160.40.50.207:8080/VisualIndexService";
		String indexColection = "webIndexPrototype";
		
		String codebook = "/home/manosetro/Desktop/learning_files/codebook.txt"; 
		String pcaFile = "/home/manosetro/Desktop/learning_files/pca.txt";
		
		DBObject query = new BasicDBObject("status", "new");
		query.put("type", "image");
	
		VisualIndexer visualIndexer;
		try {
			visualIndexer = new VisualIndexer(indexHostname, indexColection, codebook, pcaFile);
		} catch (Exception e) {
			return;
		}
		
		Updater updater = new Updater(mongoHost, mongoDbName, mongoCollectionName);
		
		TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("injector", new MongoDBInjector(mongoHost, mongoDbName, mongoCollectionName, query), 1);
        builder.setBolt("ranker", new MediaRanker(), 2).shuffleGrouping("injector");
        builder.setBolt("indexer", visualIndexer, 2).shuffleGrouping("ranker");
     
		builder.setBolt("updater", updater, 2).shuffleGrouping("indexer");
        
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("visual-indexer", conf, builder.createTopology());
        
	}

}
