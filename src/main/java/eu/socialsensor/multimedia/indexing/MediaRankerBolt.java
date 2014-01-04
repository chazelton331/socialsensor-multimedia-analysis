package eu.socialsensor.multimedia.indexing;

import java.util.Map;

import com.mongodb.DBObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class MediaRankerBolt extends BaseRichBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = -2548434425109192911L;
	private OutputCollector _collector;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("id", "url", "score", "size"));
    }

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, 
			OutputCollector collector) {
		this._collector = collector;
	}

	public void execute(Tuple tuple) {
		DBObject obj = (DBObject) tuple.getValueByField("mediaitem");
		
		String id = (String) obj.get("id");
		String url = (String) obj.get("url");
		Integer shares = (Integer) obj.get("shares");
		if(shares==null)
			shares = 0;
		
		boolean size = true;
		
		if(obj.get("width")==null || obj.get("height")==null)
				size  = false;
		
		double sharesScore = 1 - Math.exp(-0.05 * shares);
		sharesScore = (sharesScore + 1) / 2;
		
		_collector.emit(new Values(id, url, sharesScore, size));
        _collector.ack(tuple);
	}   
}