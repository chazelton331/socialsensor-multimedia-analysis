socialsensor-multimedia-analysis
================================

Contains a set of analysis processes on streams of incoming media items.

<h2>Counters</h2>
<p> Counters is a set of services that executed periodically by extending the class TimeTask of Java. In the package <b>eu.socialsensor.framework.multimedia.counters</b> there are four counters for counting contributors, domains and tags
by submiting map/reduce jobs in mongodb. 

</p>

<h2>ContributorsCounter</h2>
ContributorsCounter is based on the MapReduce operation of mongodb and counts the number of unique contributors (users) per timeslot.  

To initialize and execute contributors counter every 30 minutes run the following code:

      ContributorsCounter counter = new ContributorsCounter("hostname", "dbname", "collection", "output");
      Timer call = new Timer();
      call.scheduleAtFixedRate(counter, 0, 30*60*1000);

The service submit the the following map/reduce javascript functions and mongodb engine executes them and 
writes the results in the collection "output" </br>

<b>map function</b>

      function() {
      	var k = {stream:this.streamId, author:this.author}; 
    	emit(k, 1);
      }
      
<b>reduce function</b>    

	function(previous, current) {  
        	var count = 0;
        	for (index in current) {
        		count += current[index];
        	}
        	return count;
        }
        		
<h3>TagsCounter</h3>
The tags counter is initialized and executed in a similar manner.  

<b>map function</b>

	function() {
        	for (index in this.tags) {
        		var tag = this.tags[index]; 
        		var tmp = \"=\";
        		if(tag.length<20 && tag.indexOf(tmp) == -1) {
        			emit(tag.toLowerCase(), 1);
        		}
        	}
        }

        		
<h3>DomainsCounter</h3>
The domains counter is initialized and executed in a similar manner.

<b>map function</b>

	function() {
		var domain = this.url.match(/:\\/\\/(.[^/]+)/)[1];
		emit(domain, 1);
        }
        	


<h2>Geospatial Analysis</h2> 

The package <b>eu.socialsensor.framework.multimedia.spatial</b> contains a set of classes for the periodic clustering of geo-tagged media items based on their spatial proximity. The proximity of media items is based om the Vincenty geiodesic distance between two geo-points. The used clustering algorithm is BIRCH. 
The resulted clusters indexed in a solr collection.  

To initialize and execute birch clustering:

	Timer timer = new Timer(); 
	timer.schedule(
		new BIRCHClusterer(
			"mongoHostname", 
			"mongoDBname"
			"collection", 
			"solrHostname"), 
		0, 
		15*60*1000
	);
	

<h2>Visual Analysis</h2> 

The package <b>eu.socialsensor.framework.multimedia.visual</b> contains a set of classes for the periodic clustering of media items based on their visual similarity. The visual similarity of media items is based on a combination of SURF descriptors aggregated with the VLAD scheme. For the extraction of VLAD+SURF descriptors the implemetation of Socialsensor is used:
[multimedia-indexing](https://github.com/socialsensor/socialsensor-multimedia-analysis)
For the clustering, SCAN graph clustering algorithm is used.  

Additional information
------------------------
###Project dependencies###
The computational-verification project is dependent on two SocialSensor projects:
* [Socialsensor-framework-common](https://github.com/socialsensor/socialsensor-framework-common) : This project contains main classes and interfaces to be used by other SocialSensor projects.
* [Socialsensor-framework-client](https://github.com/socialsensor/socialsensor-framework-client) : The project contains a set of convenience methods on top of common data repositories.
* [multimedia-indexing](https://github.com/socialsensor/socialsensor-multimedia-analysis) : A framework for large-scale feature extraction, indexing and retrieval.


###Contact information###
For further details, contact Symeon Papadopoulos (papadop@iti.gr) or Manos Schinas (manosetro@iti.gr).
