package eu.socialsensor.multimedia.indexing;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import javax.imageio.ImageIO;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import eu.socialsensor.framework.client.search.visual.VisualIndexHandler;
import eu.socialsensor.visual.vectorization.ImageVectorizer;

public class TestIndexer {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		String mongoHost = "social1.atc.gr";
		String mongoDbName = "Streams"; 
		String mongoCollectionName = "MediaItems";
		
		String webServiceHost = "http://160.40.50.207:8080/VisualIndexService";
		String indexCollection = "webIndexPrototype";
		
		String codebookFile = "/home/manosetro/Desktop/learning_files/codebook.txt"; 
		String pcaFile = "/home/manosetro/Desktop/learning_files/pca.txt";
		
		ImageVectorizer vectorizer = new ImageVectorizer(codebookFile, pcaFile, false);
		VisualIndexHandler visualIndex = new VisualIndexHandler(webServiceHost, indexCollection);
		
		MongoClient m = new MongoClient(mongoHost);
		DB database = m.getDB(mongoDbName);
		DBCollection collection = database.getCollection(mongoCollectionName);
		
		DBObject obj = collection.findOne();
		
		String id = obj.get("id").toString();
		String url = obj.get("url").toString();
		
		System.out.println("Got: " + id + ", " + url);
		BufferedImage img;
		try {
			img = ImageIO.read(new URL(url));
			System.out.println("Size: " + img.getHeight()+"x"+img.getWidth());
			
			double[] vector = vectorizer.transformToVector(img);
			
			System.out.println(vector.length);
			boolean indexed = visualIndex.index(id, vector);
			
			System.out.println("indexed: " + indexed);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		
	}

}
