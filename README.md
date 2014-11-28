socialsensor-multimedia-analysis
================================

Contains a set of analysis processes on streams of incoming media items.

<h2>Counters</h2>
<p> Counters is a set of services that executed periodically by extending the class TimeTask of Java. In the package <b>eu.socialsensor.framework.multimedia.counters</b> there are four counters for counting contributors, domains, tags and items. 

</p>

<h2>ContributorsCounter</h2>
ContributorsCounter is based on the MapReduce operation of mongodb and counts the number of unique contributors (users) per timeslot.  

To initialize and execute contributors counter every 30 minutes run the following code:

      ContributorsCounter counter = new ContributorsCounter("hostname", "dbname", "collection", "output");
      Timer call = new Timer();
      call.scheduleAtFixedRate(counter, 0, 30*60*1000);

The service submit the the following map/reduce javascript functions and mongodb engine executes them and 
writes the results in the collction "output" </br>
<b>map</b>

      function() {
      	var k = {stream:this.streamId, author:this.author}; 
    	emit(k, 1);
      }
      
<b>reduce</b>    

	function(previous, current) {  
        	var count = 0;
        	for (index in current) {
        		count += current[index];
        	}
        	return count;
        }
        		
<h3>TagsCounter</h3>
In  similar manner is initialized and executed the tags counter. 

<h3>DomainsCounter</h3>


<h3>ItemsCounter</h3>








Additional information
------------------------
###Project dependencies###
The computational-verification project is dependent on two SocialSensor projects:
* [Socialsensor-framework-common](https://github.com/socialsensor/socialsensor-framework-common) : This project contains main classes and interfaces to be used by other SocialSensor projects.

###Contact information###
For further details, contact Symeon Papadopoulos (papadop@iti.gr) or Manos Schinas (manosetro@iti.gr).
