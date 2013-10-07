package eu.socialsensor.framework.multimedia.counters;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import eu.socialsensor.framework.client.dao.MediaItemDAO;
import eu.socialsensor.framework.client.dao.impl.MediaItemDAOImpl;
import eu.socialsensor.framework.common.domain.MediaItem;


public class ItemsCounter extends TimerTask {

	public static long SECONDS 	= 	1000;
	public static long MINUTES 	= 	60000;
	public static long HOURS 	= 	60 * 60000;
	
	private long resolution;
	
	private MediaItemDAO mediaItemDAO;
	
	private DB db;
	private DBCollection output_coll;
	
	private long lastPublicationTime = 0;
	
	public ItemsCounter(long resolution, String host, String dbname, String input_coll, String output) 
			throws Exception  {
		if(resolution!=SECONDS && resolution!=MINUTES && resolution!=HOURS) {
			throw new Exception("Resolution must be seconds, minutes or hours");
		}
		this.resolution = resolution;
		
		MongoClient mongo = new MongoClient(host, 27017);
        this.db = mongo.getDB(dbname);
        
        mediaItemDAO = new MediaItemDAOImpl(host, dbname, input_coll);
        
        this.output_coll = db.getCollection(output);
	}
	
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		String host = "127.0.0.1";
		String db = "first_prototype";
		String input = "MediaItems";
		String output = "histogram";
		
		ItemsCounter counter = new ItemsCounter(ItemsCounter.HOURS, host, db, input, output);
		
		Timer timer = new Timer(); 
		timer.schedule(counter, (long)1000, (long)30000);
	}


	@Override
	public void run() {
		
		Map<Long, Integer> histogram = new HashMap<Long, Integer>();
		long firstBin = getBin(lastPublicationTime);
		List<MediaItem> mediaItems = mediaItemDAO.getLastMediaItems(firstBin);
		for(MediaItem mediaItem : mediaItems) {
	
			long publicationTime = mediaItem.getPublicationTime();
			long bin_index = getBin(publicationTime);
			
			Integer count = histogram.get(bin_index);
			if(count == null) {
				count = 0;
			}
			histogram.put(bin_index, ++count);
		}
		
		Set<Long> bins = histogram.keySet();
		if(!bins.isEmpty())
			lastPublicationTime = Collections.max(bins);
		for(Long bin : bins) {
			DBObject q = new BasicDBObject("_id", bin);
			
			Integer count = histogram.get(bin);
			DBObject o = new BasicDBObject("_id", bin);
			o.put("timestamp", bin);
			o.put("count", count);
			o.put("date", new Date(bin));
			
			output_coll.update(q, o, true, false);
		}
	}

	private long getBin(long timestamp) {
		long temp = (long) Math.floor(timestamp / resolution);
		long bin_index =  resolution * temp;
		return bin_index;	
	}
}
