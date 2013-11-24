package eu.socialsensor.framework.multimedia.visual;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.socialsensor.framework.client.dao.MediaItemDAO;
import eu.socialsensor.framework.client.dao.impl.MediaItemDAOImpl;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.MediaItem;
import eu.socialsensor.framework.common.domain.dysco.Dysco;
import eu.socialsensor.framework.common.services.GenericDyscoCreator;
import eu.socialsensor.framework.multimedia.visual.clustering.VisualClusterer;

public class MultimediaDyscoCreator implements GenericDyscoCreator {

	private MediaItemDAO mediaItemDAO;
	//private ItemDAO itemDAO;
	private VisualClusterer visualClusterer;
	
	
	public MultimediaDyscoCreator(String mongoDbHost, String visualIndexHost, String indexCollection) {
		mediaItemDAO = new MediaItemDAOImpl(mongoDbHost, "first_prototype_2");
		//itemDAO = new ItemDAOImpl(mongoDbHost);
		visualClusterer = new VisualClusterer(visualIndexHost, indexCollection);
	}
	
	@Override
	public List<Dysco> createDyscos(List<Item> items) {
			
		List<Dysco> dyscos = new ArrayList<Dysco>();	
		
		List<MediaItem> mediaItems = new ArrayList<MediaItem>();
		Map<String, Item> itemsMap = new HashMap<String, Item>();
		for(Item item : items) {
			itemsMap.put(item.getId(), item);
			List<String> mediaIds = item.getMediaIds();
			for(String mediaItemId : mediaIds) {
				MediaItem mediaItem = mediaItemDAO.getMediaItem(mediaItemId);
				mediaItems.add(mediaItem);
			}
		}
			
		List<List<String>> clusters = visualClusterer.cluster(mediaItems);		
		for(List<String> cluster : clusters) {
			List<Item> clusterItems = new ArrayList<Item>();
			for(String itemId : cluster) {
				Item item = itemsMap.get(itemId); //itemDAO.getItem(itemId);
				if(item != null) {
					clusterItems.add(item);
				}
			}
				
			Dysco dysco = new Dysco(); 
				
			/*
			 * TODO Extract fields of Dysco: Title, keywords, etc
			*/
				
			dysco.setItems(items);
			dyscos.add(dysco);
		}
		return dyscos;
	}

	
}
