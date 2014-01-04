package eu.socialsensor.multimedia.indexing;

import java.awt.image.BufferedImage;
import java.net.URL;
import java.util.Map;

import javax.imageio.ImageIO;

import eu.socialsensor.framework.client.search.visual.VisualIndexHandler;
import gr.iti.mklab.visual.aggregation.VladAggregatorMultipleVocabularies;
import gr.iti.mklab.visual.dimreduction.PCA;
import gr.iti.mklab.visual.extraction.AbstractFeatureExtractor;
import gr.iti.mklab.visual.extraction.SURFExtractor;
import gr.iti.mklab.visual.vectorization.ImageVectorization;
import gr.iti.mklab.visual.vectorization.ImageVectorizationResult;
import static backtype.storm.utils.Utils.tuple;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class VisualIndexerBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5514715036795163046L;


	private OutputCollector _collector;
	private VisualIndexHandler visualIndex;

	private String webServiceHost;
	private String indexCollection;

	private static int[] numCentroids = { 128, 128, 128, 128 };
	private static int targetLengthMax = 1024;
	
	private static int maxNumPixels = 768 * 512; // use 1024*768 for better/slower extraction
	
	public VisualIndexerBolt(String webServiceHost, String indexCollection, String[] codebookFiles, String pcaFile) throws Exception {
		
		this.webServiceHost = webServiceHost;
		this.indexCollection = indexCollection;
		
		ImageVectorization.setFeatureExtractor(new SURFExtractor());
		
		VladAggregatorMultipleVocabularies vladAggregator = new VladAggregatorMultipleVocabularies(codebookFiles, numCentroids, 
				AbstractFeatureExtractor.SURFLength);
		
		ImageVectorization.setVladAggregator(vladAggregator);
		
		int initialLength = numCentroids.length * numCentroids[0] * AbstractFeatureExtractor.SURFLength;
		if(initialLength > targetLengthMax) {
			PCA pca = new PCA(targetLengthMax, 1, initialLength, true);
			pca.loadPCAFromFile(pcaFile);
			ImageVectorization.setPcaProjector(pca);
		}
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector = collector;
		this.visualIndex = new VisualIndexHandler(webServiceHost, indexCollection);
	}

	public void execute(Tuple tuple) {
		
		String id = tuple.getStringByField("id");
		String url = tuple.getStringByField("url");
		Double score = tuple.getDoubleByField("score");
		
		boolean size = tuple.getBooleanByField("size");
		
		System.out.println("Fetch and extract feature vector for " + id + " with score " + score);
		try {
			
			
			BufferedImage image = ImageIO.read(new URL(url));
			
			Integer width=-1, height=-1;
			boolean indexed = false;
			if(image != null) {
				
				ImageVectorization imvec = new ImageVectorization(id, image, targetLengthMax, maxNumPixels);
				
				if(!size) {
					width = image.getWidth();
					height = image.getHeight();
				}

				ImageVectorizationResult imvr = imvec.call();
				double[] vector = imvr.getImageVector();

			
				indexed = visualIndex.index(id, vector);

			}
			
			if(indexed) {
				_collector.emit(tuple(id, Boolean.TRUE, width, height));
			}
			else {
				_collector.emit(tuple(id, Boolean.FALSE, width, height));
			}
		} 
		catch (Exception e) {
			System.out.println("Exception: " + e.getMessage());
			_collector.emit(tuple(id, Boolean.FALSE, -1, -1));
			return;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "indexed", "width", "height"));
	}

//	private byte[] fetch(String urlStr) throws IOException {
//		URL url = new URL(urlStr);
//		StringWriter writer = new StringWriter();
//		IOUtils.copy(new InputStreamReader(url.openStream()), writer);
//		
//		byte[] bytes = writer.toString().getBytes();
//		return bytes;
//	}
}
