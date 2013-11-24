package eu.socialsensor.multimedia.indexing;

import java.awt.image.BufferedImage;
import java.net.URL;
import java.util.Map;

import javax.imageio.ImageIO;

import eu.socialsensor.framework.client.search.visual.VisualIndexHandler;
import eu.socialsensor.visual.vectorization.ImageVectorizer;

import static backtype.storm.utils.Utils.tuple;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class VisualIndexer extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5514715036795163046L;


	private OutputCollector _collector;
	private VisualIndexHandler visualIndex;
	private ImageVectorizer vectorizer;

	private String webServiceHost;
	private String indexCollection;

	public VisualIndexer(String webServiceHost, String indexCollection, String codebookFile, String pcaFile) throws Exception {
		
		this.webServiceHost = webServiceHost;
		this.indexCollection = indexCollection;
		
		this.vectorizer = new ImageVectorizer(codebookFile, pcaFile, false);
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
			if(vectorizer == null) {
				System.out.println("Vectorizer is null");
				_collector.emit(tuple(id, Boolean.FALSE, -1, -1));
				return;
			}
			
			BufferedImage img = ImageIO.read(new URL(url));
			
			Integer width=-1, height=-1;
			boolean indexed = false;
			if(img != null) {
				
				if(!size) {
					width = img.getWidth();
					height = img.getHeight();
				}
//			byte[] imageBytes = fetch(url);
//			InputStream is = new ByteArrayInputStream(imageBytes);
//			BufferedImage img = ImageIO.read(is);

				double[] vector = vectorizer.transformToVector(img);
				//System.out.println("Vector length: " + vector.length);
			
				indexed = visualIndex.index(id, vector);
				//System.out.println("Indexed: " + indexed);
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
