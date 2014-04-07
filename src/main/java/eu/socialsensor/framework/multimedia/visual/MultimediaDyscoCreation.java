package eu.socialsensor.framework.multimedia.visual;

import java.util.List;

import eu.socialsensor.framework.client.dao.ItemDAO;
import eu.socialsensor.framework.client.dao.impl.ItemDAOImpl;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.dysco.Dysco;

public class MultimediaDyscoCreation implements Runnable {

	private ItemDAO itemDAO;
	
	MultimediaDyscoCreator dyscoCreator;
	
	public MultimediaDyscoCreation(String mongoDbHost, String mongoDb, String visualIndexHost, String indexCollection) throws Exception {
		itemDAO = new ItemDAOImpl(mongoDbHost, mongoDb);
		dyscoCreator = new MultimediaDyscoCreator(mongoDbHost, visualIndexHost, indexCollection);
	}
	
	@Override
	public void run() {
		while(true) {
			List<Item> items = itemDAO.getLatestItems(10000);
			System.out.println("Last items: " + items.size());
			
			List<Dysco> dyscos = dyscoCreator.createDyscos(items);
			System.out.println("Total dyscos found: " + dyscos.size());
			for(Dysco dysco : dyscos) {
				System.out.println("Items of dysco " + dysco.getId() + " " +  
						dysco.getItems().size());
			}
			
			return;
		}
		
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Thread thread = new Thread(new MultimediaDyscoCreation(
				"160.40.50.207", "Streams", "http://160.40.50.207:8080", "mmdemo"));
		
		thread.start();
		thread.join();
	}
	
}
