package eu.socialsensor.framework.multimedia;

import java.util.Timer;

import eu.socialsensor.framework.multimedia.counters.ContributorsCounter;
import eu.socialsensor.framework.multimedia.counters.DomainsCounter;
import eu.socialsensor.framework.multimedia.counters.ItemsCounter;
import eu.socialsensor.framework.multimedia.counters.TagsCounter;
import eu.socialsensor.framework.multimedia.spatial.clustering.BIRCHClusterer;
import eu.socialsensor.framework.multimedia.visual.clustering.VisualClusterer;



public class MultimediaAnalysis {

	
	public static void main(String[] args) {
		
		String host = "160.40.50.207";
		String db = "mmdemo";
		String input = "MediaItems";
		String output = "histogram";
		
		String solr_host = "http://160.40.50.207:8080/solr-4.2.1/";
		
		Timer timer = new Timer(); 

//		WebItemsIndexer webItemsIndexer;
//		try {
//			webItemsIndexer = new WebItemsIndexer(host, db, input, solr_host);
//			timer.schedule(webItemsIndexer, (long)100, (long)10*60*60000);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		
		try {
			DomainsCounter domainsCounter = new DomainsCounter(host, db, input, "domains");
			timer.schedule(domainsCounter, (long)100, (long)30*60000);
		} catch (Exception e) {
			e.printStackTrace();
		}
//
		try {
			ContributorsCounter contributorsCounter = new ContributorsCounter(host, db, input, "contributors");
			timer.schedule(contributorsCounter, (long)100, (long)10*60000);
		} catch (Exception e) {
			e.printStackTrace();
		}

		ItemsCounter hours_counter;
		try {
			hours_counter = new ItemsCounter(ItemsCounter.HOURS, host, db, input, "hours_"+output);
			timer.schedule(hours_counter, (long)5000, (long)10*60*1000);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		ItemsCounter minutes_counter;
		try {
			minutes_counter = new ItemsCounter(ItemsCounter.MINUTES, host, db, input, "minutes_"+output);
			timer.schedule(minutes_counter, (long)10000, (long)10000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		ItemsCounter seconds_counter;
		try {
			seconds_counter = new ItemsCounter(ItemsCounter.SECONDS, host, db, input, "seconds_"+output);
			timer.schedule(seconds_counter, (long)15000, (long)5000);
		} catch (Exception e) {
			e.printStackTrace();
		}
//		
//		BIRCHClusterer hotspotsClustering;
//		try {
//			hotspotsClustering = new BIRCHClusterer(host, db, input, solr_host+"Hotspots");
//			timer.schedule(hotspotsClustering, (long)100, (long)15*60000);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		

		try {
			TagsCounter tagsCounter = new TagsCounter(host, db, input, "tags");
			timer.schedule(tagsCounter, (long)100, (long)10*60000);
		} catch (Exception e) {
			e.printStackTrace();
		}


//		WebItemsVisualIndexer webItemsVisualIndexer;
//		try {
//			webItemsVisualIndexer = new WebItemsVisualIndexer(host, db, input, 
//					"http://160.40.50.207:8080/socialsensorVisualIndex/");
//			timer.schedule(webItemsVisualIndexer, (long)100, (long)40*60*60000);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		
	}

}
