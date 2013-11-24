package eu.socialsensor.framework.multimedia;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimerTask;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class WebItemsIndexer extends TimerTask {

	private MongoClient mongo;
	private DB db;
	private DBCollection input_collection;
	private HttpSolrServer solr;

	Set<String> seedlists = new HashSet<String>();
	
	WebItemsIndexer(String host, String dbname, String collectionName, String solrServerHost) 
			throws UnknownHostException {
		this.mongo = new MongoClient(host);
		this.db = mongo.getDB(dbname);
		this.input_collection = db.getCollection(collectionName);
		
		
		this.solr = new HttpSolrServer(solrServerHost+"MediaItems");
	}
	
	@Override
	public void run() {
		DBObject query = new BasicDBObject("streamId", "Flickr");
		
		DBCursor items = input_collection.find(query).sort(new BasicDBObject("publicationTime",-1));
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		
		while(items.hasNext()) {
			DBObject item = items.next();
			
			String id = (String) item.get("id");
			String text = (String) item.get("title");
			
			BasicDBList tags =  (BasicDBList) item.get("tags");
			if(tags!=null && tags.size()>0) {
				for(Object tag : tags) {
					text += (" "+tag);
				}
				//System.out.println(text);
			}
			
			String stream = (String) item.get("streamId");
			Boolean hasGeo = false;
			if(item.get("location.coordinates") != null ) {
				hasGeo = true;
			}
			long time = (Long) item.get("publicationTime");
			
			
			if(id!=null && text!=null && stream!=null) {
				
				SolrInputDocument doc = new SolrInputDocument();
				doc.addField("id", id);
				doc.addField("time", time);
				doc.addField("text", text);
				doc.addField("stream", stream);
				doc.addField("geo", hasGeo);
				
				docs.add(doc);
				
			}
			
		}
		
		System.out.println(docs.size() + " Web media items to index!");
		try {
			solr.add(docs);
			solr.commit();
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
