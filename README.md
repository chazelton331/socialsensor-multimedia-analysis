socialsensor-multimedia-analysis
================================

Contains a set of analysis processes on streams of incoming media items.

<h1>Counters</h1>
<p> Counters is a set of services that executed periodically by extending the class TimeTask of Java. In the package <b>eu.socialsensor.framework.multimedia.counters</b> there are four counters for counting contributors, domains, tags and items. 

</p>

<h2>ContributorsCounter</h2>
ContributorsCounter is based on the MapReduce operation of mongodb and counts the number of unique contributors (users) per timeslot.  










Additional information
------------------------
###Project dependencies###
The computational-verification project is dependent on two SocialSensor projects:
* [Socialsensor-framework-common](https://github.com/socialsensor/socialsensor-framework-common) : This project contains main classes and interfaces to be used by other SocialSensor projects.

###Contact information###
For further details, contact Symeon Papadopoulos (papadop@iti.gr) or Manos Schinas (manosetro@iti.gr).
