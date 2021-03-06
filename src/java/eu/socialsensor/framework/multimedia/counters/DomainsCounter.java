package eu.socialsensor.framework.multimedia.counters;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.TimerTask;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceCommand.OutputType;
import com.mongodb.MongoClient;

public class DomainsCounter extends TimerTask {

	private DBCollection collection;
	private OutputType outputType;

	private MapReduceCommand mr_cmd;
	//private String output;

	private static String map = "function() { " +
			"   var domain = this.url.match(/:\\/\\/(.[^/]+)/)[1]; " + 
			"   if(domain.indexOf('flickr.com', domain.length - 'flickr.com'.length) !== -1) domain='flickr.com'; "+
			"   if(domain.indexOf('fbcdn.net', domain.length - 'fbcdn.net'.length) !== -1) domain='fbcdn.net'; "+
        	"	emit(domain, 1); " +
        	"}";
	
	private static String reduce = "function(previous, current) { " + 
        		"var count = 0; " +
        		"for (index in current) { " +
        		"	count += current[index];" +
        		"}" +
        		"return count;" +
        		"}";

	public DomainsCounter(String host, String dbname, String collectionName, String output) throws Exception {
		MongoClient mongo = null;
		try {
			mongo = new MongoClient(host, 27017);
		} catch (UnknownHostException e) {
			throw new Exception(e);
		}
		DB db = mongo.getDB(dbname);
		this.collection = db.getCollection(collectionName);
		
		this.outputType = MapReduceCommand.OutputType.REPLACE;
		
		DBObject query = new BasicDBObject();
		query.put("url", new BasicDBObject("$exists", Boolean.TRUE));
				
		//this.output = output;
		this.mr_cmd = new MapReduceCommand(collection, map, reduce, output, outputType, query);
		
	}
	
	
	public static void main(String[] args) {

			
	}

	@Override
	public void run() {
		System.out.println("Run Map/Reduce Domains Counter job: " + new Date());
		collection.mapReduce(mr_cmd);
		System.out.println("Done.");
	}
}