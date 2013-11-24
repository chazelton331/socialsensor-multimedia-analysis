package eu.socialsensor.framework.multimedia;

import java.awt.image.BufferedImage;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import javax.imageio.ImageIO;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.Bytes;

import eu.socialsensor.framework.client.search.visual.VisualIndexHandler;
import gr.iti.mklab.visual.aggregation.VladAggregatorMultipleVocabularies;
import gr.iti.mklab.visual.dimreduction.PCA;
import gr.iti.mklab.visual.extraction.AbstractFeatureExtractor;
import gr.iti.mklab.visual.extraction.SURFExtractor;
import gr.iti.mklab.visual.vectorization.ImageVectorization;
import gr.iti.mklab.visual.vectorization.ImageVectorizationResult;


public class WebItemsVisualIndexer implements Runnable {

	private MongoClient mongo;
	private DB db;
	private DBCollection input_collection;
	
	private VisualIndexHandler visualIndexHandler;
	private String learningFolder = "C:/Users/lef/Desktop/ITI/data/learning_files/best_files_4-11-2013/";
	private String[] codebookFiles = { learningFolder + "surf_l2_128c_0.csv",
			learningFolder + "surf_l2_128c_1.csv", learningFolder + "surf_l2_128c_2.csv",
			learningFolder + "surf_l2_128c_3.csv" };
	private String pcaFile = "";

	private String indexCollection;
	private ArrayBlockingQueue<DBObject> queue;
	
	private static int[] numCentroids = { 128, 128, 128, 128 };
	private static int targetLengthMax = 1024;
	
	private static int maxNumPixels = 768 * 512; // use 1024*768 for better/slower extraction
	
	WebItemsVisualIndexer(String host, String dbname, String collectionName, 
			String webServiceHost) 
			throws Exception {
		this.mongo = new MongoClient(host);
		this.db = mongo.getDB(dbname);
		this.input_collection = db.getCollection(collectionName);
		
		input_collection.addOption(Bytes.QUERYOPTION_NOTIMEOUT);
		
		ImageVectorization.setFeatureExtractor(new SURFExtractor());
		VladAggregatorMultipleVocabularies vladAggregator = new VladAggregatorMultipleVocabularies(codebookFiles, numCentroids, 
				AbstractFeatureExtractor.SURFLength);
		
		ImageVectorization.setVladAggregator(vladAggregator);
		int initialLength = numCentroids.length * numCentroids[0] * AbstractFeatureExtractor.SURFLength;
		PCA pca = new PCA(targetLengthMax, 1, initialLength, true);
		pca.loadPCAFromFile(pcaFile);
		ImageVectorization.setPcaProjector(pca);
		
		this.indexCollection = "mmdemo";
		visualIndexHandler = new VisualIndexHandler(webServiceHost, indexCollection);
		
		this.queue = new ArrayBlockingQueue<DBObject>(10000);
		
		List<Thread> fetchers = new ArrayList<Thread>();
		for(int i=0; i<10; i++) {
			Thread t = new Thread(new Fetcher(queue, visualIndexHandler, input_collection));
			fetchers.add(t);
		}
		
		for(Thread t : fetchers)
			t.start();
		
	}
	
	@Override
	public void run() {
		while(true) {
			
			
			
			DBObject query = new BasicDBObject();
			query.put("indexed", false);
			query.put("status", "new");
			query.put("type", "image");
			query.put("streamId", "Twitter");
			
			System.out.print("Find: " + query);
			DBCursor items = input_collection.find(query).sort(new BasicDBObject("url", 1));
			System.out.println(" ... DONE");
			
			int size = items.size();
			System.out.println("Size: " + size);
			if(size == 0) {
				try {
					Thread.sleep(5 * 60 * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				continue;
			}
			
			int k=0;
			while(items.hasNext()) {
				DBObject item = items.next();
				
				try {
					queue.put(item);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				
				if(++k%500==0) {
					break;
				}
			
			}
		}
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
		String mongoDb = "MMdemoStreams";
		String mongoCollection = "MediaItems";
		
		String visualIndexService = "http://160.40.50.207:8080/VisualIndexService";
		
		WebItemsVisualIndexer indexer = new WebItemsVisualIndexer(mongoHost, mongoDb, mongoCollection, visualIndexService);
		
		Thread thread = new Thread(indexer);
		thread.start();
		try {
			thread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public class Fetcher implements Runnable{

		private ArrayBlockingQueue<DBObject> queue;

		private VisualIndexHandler handler;
		private DBCollection collection;

		public Fetcher(ArrayBlockingQueue<DBObject> queue, VisualIndexHandler handler, DBCollection collection) {
			this.queue = queue;
			this.handler = handler;
			this.collection = collection;
		}
		
		@Override
		public void run() {
			while(true) {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e2) {
					e2.printStackTrace();
				}
				
				DBObject item;
				try {
					item = queue.take();
				} catch (InterruptedException e1) {
					e1.printStackTrace();
					continue;
				}
				
				String id = (String) item.get("id");
				String url = (String) item.get("url");
				
				System.out.print(id + " => "+url + " ");
				
				try {
					InputStream is = new URL(url).openStream();
					BufferedImage image = ImageIO.read(is);
					
					ImageVectorization imvec = new ImageVectorization(id, image, targetLengthMax, maxNumPixels);
					ImageVectorizationResult imvr = imvec.call();
					double[] vector = imvr.getImageVector();
					
					boolean indexed = false;
					synchronized(handler) {
						indexed = handler.index(id, vector);
					}
					if(indexed) {
						System.out.println(" ok");
						DBObject q = new BasicDBObject("id", id);
						DBObject f = new BasicDBObject();
						f.put("indexed",true);
						f.put("status", "done");

						DBObject o = new BasicDBObject("$set", f);		
						collection.update(q , o , false, true);
					}
					else {
						System.out.println(" not ok");
						DBObject q = new BasicDBObject("id", id);
						DBObject o = new BasicDBObject("$set", 
								new BasicDBObject("status", "error"));		
						collection.update(q , o , false, true);
					}
					
					
					
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println(" not ok");
					DBObject q = new BasicDBObject("id", id);
					DBObject o = new BasicDBObject("$set", 
							new BasicDBObject("status", "error"));		
					collection.update(q , o , false, true);
					continue;
				}
			}
			
		}
		
	}
}
