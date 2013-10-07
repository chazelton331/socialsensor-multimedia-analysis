package eu.socialsensor.framework.multimedia;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.imageio.ImageIO;

import org.apache.commons.io.IOUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.Bytes;

import eu.socialsensor.framework.client.search.visual.VisualIndexHandler;
import eu.socialsensor.visual.vectorization.ImageVectorizer;

public class WebItemsVisualIndexer implements Runnable {

	private MongoClient mongo;
	private DB db;
	private DBCollection input_collection;
	
	private VisualIndexHandler visualIndexHandler;
	private String codebook;
	private String pca;
	private ImageVectorizer vectorizer;
	private String indexCollection;
	
	WebItemsVisualIndexer(String host, String dbname, String collectionName, 
			String webServiceHost) 
			throws Exception {
		this.mongo = new MongoClient(host);
		this.db = mongo.getDB(dbname);
		this.input_collection = db.getCollection(collectionName);
		
		input_collection.addOption(Bytes.QUERYOPTION_NOTIMEOUT);

		this.codebook = "/home/manosetro/Desktop/learning_files/codebook.txt";
		this.pca = "/home/manosetro/Desktop/learning_files/pca.txt";
		
		this.indexCollection = "mmdemo";
		
		this.vectorizer = new ImageVectorizer(codebook, pca, false);
		
		visualIndexHandler = new VisualIndexHandler(webServiceHost, indexCollection);
	}
	
	@Override
	public void run() {
		while(true) {
			DBObject query = new BasicDBObject();
			query.put("indexed", false);
			query.put("status", "new");
			query.put("type", "image");
		
			System.out.print("Find: " + query);
			DBCursor items = input_collection.find(query);
			System.out.println(" ... DONE");
			
			int size = items.size();
			System.out.println("Size: " + size);
			if(size == 0) {
				break;
			}
			
			int k=0;
			while(items.hasNext()) {
				DBObject item = items.next();
				
				String id = (String) item.get("id");
				String url = (String) item.get("url");
				
				if(++k%500==0) {
					break;
				}
				System.out.print(k+") "+id + " => "+url + " ");
				
				try {
					InputStream is = new URL(url).openStream();
					//byte[] content = IOUtils.toByteArray(is);
					
					BufferedImage image = ImageIO.read(is);
					
					double[] vector = vectorizer.transformToVector(image);
					
			
					if(visualIndexHandler.index(id, vector)) {
						System.out.println(" ok");
						DBObject q = new BasicDBObject("id", id);
						DBObject o = new BasicDBObject("$set", 
								new BasicDBObject("indexed",true));		
						input_collection.update(q , o , false, true);
					}
					else {
						System.out.println(" not ok");
						DBObject q = new BasicDBObject("id", id);
						DBObject o = new BasicDBObject("$set", 
								new BasicDBObject("status", "error"));		
						input_collection.update(q , o , false, true);
					}
					
					
					
				} catch (Exception e) {
					System.out.println(" not ok");
					DBObject q = new BasicDBObject("id", id);
					DBObject o = new BasicDBObject("$set", 
							new BasicDBObject("status", "error"));		
					input_collection.update(q , o , false, true);
					continue;
				}
				
				
			}
		}
		System.out.println("DONE");
	}

//	private boolean indexImage(String id) {
//		GetMethod indexMethod = null;
//  	    int response = -1;
//  	    try {
//			indexMethod = new GetMethod(webServiceHost + "rest/vector/query_similar?id="+id);
//			response = httpClient.executeMethod(indexMethod);
//			
//			System.out.println(id+" => "+response);
//			System.out.println(indexMethod.getResponseBodyAsString());
//			
//		} catch(Exception e) {
//			e.printStackTrace();
//		} finally {
//  	  		if(indexMethod != null)
//  	  			indexMethod.releaseConnection();
//  	  	}
//  	    return response==200 ? true : false;
//  	}
	
	public static void main(String[] args) throws Exception {
		
		String mongoHost = "160.40.50.207";
		String mongoDb = "mmdemo";
		String mongoCollection = "MediaItems";
		
		String visualIndexService = "http://localhost:8080/VisualIndexService";
		
		WebItemsVisualIndexer indexer = new WebItemsVisualIndexer(mongoHost, mongoDb, mongoCollection, visualIndexService);
		
		Thread thread = new Thread(indexer);
		thread.start();
		try {
			thread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
