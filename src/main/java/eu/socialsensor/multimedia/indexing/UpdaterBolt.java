package eu.socialsensor.multimedia.indexing;

import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


public class UpdaterBolt extends BaseRichBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = -2548434425109192911L;
	private String mongoHost;
	private String mongoDbName;
	private String mongoCollectionName;
	private MongoClient _mongo;
	private DB _database;
	private DBCollection _collection;

	public UpdaterBolt(String mongoHost, String mongoDbName, String mongoCollectionName) {
		this.mongoHost = mongoHost;
		this.mongoDbName = mongoDbName;
		this.mongoCollectionName = mongoCollectionName;
	}
	
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		
		try {
			_mongo = new MongoClient(mongoHost);
			_database = _mongo.getDB(mongoDbName);
			_collection = _database.getCollection(mongoCollectionName);
		} catch (Exception e) {
			
		}
		
	}

	public void execute(Tuple tuple) {
		String id = tuple.getStringByField("id");
		boolean indexed = tuple.getBooleanByField("indexed");
		Integer width = tuple.getIntegerByField("width");
		Integer height = tuple.getIntegerByField("height");
		
		if(_collection != null) {
			DBObject q = new BasicDBObject("id", id);
			
			BasicDBObject f = new BasicDBObject("indexed", indexed);
			if(indexed)
				f.put("status", "indexed");
			else
				f.put("status", "failed");
			
			if(width!=null && height!=null && width!=-1 && height!=-1) {
				f.put("height", height);
				f.put("width", width);
			}
			
			DBObject o = new BasicDBObject("$set", f);
			
			
			WriteResult result = _collection.update(q, o, false, true);
			if(result.getN()>0) {
				System.out.println("Update " + q + " with " + o);
			}
		}
	}   
}