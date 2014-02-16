package eu.socialsensor.framework.multimedia;

import java.util.Timer;

import eu.socialsensor.framework.multimedia.counters.ContributorsCounter;
import eu.socialsensor.framework.multimedia.counters.DomainsCounter;
import eu.socialsensor.framework.multimedia.counters.TagsCounter;

public class MultimediaAnalysis {

	
	public static void main(String[] args) {
		
		String host = args[0];
		String db = args[1];
		String input = args[2];
		
		Timer timer = new Timer(); 
		
		try {
			DomainsCounter domainsCounter = new DomainsCounter(host, db, input, "domains");
			timer.schedule(domainsCounter, (long)100, (long)30*60000);
			
			ContributorsCounter contributorsCounter = new ContributorsCounter(host, db, input, "contributors");
			timer.schedule(contributorsCounter, (long)100, (long)10*60000);
			
			TagsCounter tagsCounter = new TagsCounter(host, db, input, "tags");
			timer.schedule(tagsCounter, (long)100, (long)10*60000);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		

	}

}
