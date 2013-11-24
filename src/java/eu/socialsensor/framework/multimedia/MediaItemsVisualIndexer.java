package eu.socialsensor.framework.multimedia;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
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

import gr.iti.mklab.visual.aggregation.VladAggregatorMultipleVocabularies;
import gr.iti.mklab.visual.dimreduction.PCA;
import gr.iti.mklab.visual.extraction.AbstractFeatureExtractor;
import gr.iti.mklab.visual.extraction.SURFExtractor;
import gr.iti.mklab.visual.vectorization.ImageVectorization;
import gr.iti.mklab.visual.vectorization.ImageVectorizationResult;

public class MediaItemsVisualIndexer implements Runnable {

	private MongoClient mongo;
	private DB db;
	private DBCollection input_collection;
	
	private VisualIndexHandler visualIndexHandler;
	
	private static int[] numCentroids = { 128, 128, 128, 128 };
	private static int targetLengthMax = 1024;
	
	private static int maxNumPixels = 768 * 512; // use 1024*768 for better/slower extraction
	
	MediaItemsVisualIndexer(String host, String dbname, String collectionName, 
			String webServiceHost, String indexCollection, String[] codebookFiles, String pcaFile) 
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
		
		this.visualIndexHandler = new VisualIndexHandler(webServiceHost, indexCollection);
	}
	
	@Override
	public void run() {
		while(true) {
			DBObject query = new BasicDBObject();
			query.put("indexed", false);
			query.put("type", "image");
			
		
			DBCursor items = input_collection.find(query);
			if(items.size()==0)
				break;
			
			int k=0;
			for(DBObject item : items) {
				String id = (String) item.get("id");
				String url = (String) item.get("url");
				
				
				try {
					InputStream is = new URL(url).openStream();
					byte[] content = IOUtils.toByteArray(is);
					
					if(++k%100==0) {
						System.out.println(k + " media items indexed.");
						break;
					}
					
					BufferedImage image = ImageIO.read(new ByteArrayInputStream(content));
					
					ImageVectorization imvec = new ImageVectorization(id, image, targetLengthMax, maxNumPixels);
					ImageVectorizationResult imvr = imvec.call();
					double[] vector = imvr.getImageVector();
					
					if(visualIndexHandler.index(id, vector)) {
						DBObject q = new BasicDBObject("id", id);
						DBObject o = new BasicDBObject("$set", 
								new BasicDBObject("indexed",true));		
						input_collection.update(q , o , false, true);
					}
				} catch (Exception e) {
					continue;
				}
				
				
			}
		}
		System.out.println("DONE: All");
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
		
		String mongoHost = "localhost";
		String mongoDb = "Streams";
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
