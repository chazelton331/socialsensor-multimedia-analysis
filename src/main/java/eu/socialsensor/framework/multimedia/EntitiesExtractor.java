package eu.socialsensor.framework.multimedia;

import java.io.ByteArrayInputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang.StringEscapeUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import eu.socialsensor.framework.client.dao.ItemDAO;
import eu.socialsensor.framework.client.dao.impl.ItemDAOImpl;
import eu.socialsensor.framework.common.domain.Item;
import eu.socialsensor.framework.common.domain.dysco.Entity;

public class EntitiesExtractor {

	String model = "/media/manos/Data/workspace/EntitiesExtractor/src/main/resources/classifiers/english.all.3class.caseless.distsim.crf.ser.gz";
	
	@SuppressWarnings("rawtypes")
	AbstractSequenceClassifier classifier = CRFClassifier.getClassifierNoExceptions(model);

	public static void main(String[] args) throws Exception {
				
		EntitiesExtractor extractor = new EntitiesExtractor();
		ItemDAO itemDAO = new ItemDAOImpl("localhost", "loc", "Items");
		while(true) {
		List<Item> items = itemDAO.getItemsSince(0);
		for(Item item : items) {
			String title = item.getTitle();
			try {
				List<Entity> entities = extractor.getEntities(title);
				item.setEntities(entities);
				
				itemDAO.updateItem(item);
			} catch (Exception e) {
				continue;
			}
		}
		
		Thread.sleep(5000);
		}
//		
//		List<Entity> entities;
//		try {
//			entities = extractor.getEntities(items);
//			for (Entity entity : entities) {
////	            System.out.println(entity.getName() + " - " + entity.getType() + 
////	            		" - " + entity.getCont());
//	            System.out.println(entity.toJSONString());
//	        }
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		
	}

	public List<Entity> getEntities(List<String> items) throws ParserConfigurationException{
		Map<String, Entity> entities = new HashMap<String, Entity>();
		
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		
		for (String text : items) {
			String itemXML = classifier.classifyWithInlineXML(StringEscapeUtils.escapeXml(text));
			ByteArrayInputStream bis = new ByteArrayInputStream(("<DOC>" + itemXML + "</DOC>").getBytes());
			Document doc;
			try {
				doc = db.parse(bis);
			} catch (Exception e) {
				continue;
			}
			addEntities(entities, doc, Entity.Type.PERSON);
			addEntities(entities, doc, Entity.Type.LOCATION);
			addEntities(entities, doc, Entity.Type.ORGANIZATION);
		}
	       
		return new ArrayList<Entity>(entities.values());
	}
	
	public List<Entity> getEntities(String text) throws Exception{
		Map<String, Entity> entities = new HashMap<String, Entity>();
		
		String textXML = classifier.classifyWithInlineXML(StringEscapeUtils.escapeXml(text));

		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBl = dbf.newDocumentBuilder();
        
		ByteArrayInputStream bis = new ByteArrayInputStream(("<DOC>" + textXML + "</DOC>").getBytes());
        Document doc;
		try {
			doc = docBl.parse(bis);
		} catch (Exception e) {
			return null;
		}
		
        addEntities(entities, doc, Entity.Type.PERSON);
        addEntities(entities, doc, Entity.Type.LOCATION);
        addEntities(entities, doc, Entity.Type.ORGANIZATION);
        
		return new ArrayList<Entity>(entities.values());
	}
	
	private static void addEntities(Map<String, Entity> entities, Document doc, 
			Entity.Type tag) {
        String key;
        NodeList nodeList = doc.getElementsByTagName(tag.name());

        for (int i = 0; i < nodeList.getLength(); i++) {
            key = tag.name() + "&&&" + nodeList.item(i).getTextContent().toLowerCase();

            if (entities.containsKey(key)) {
                Entity e = entities.get(key);
                e.setCont(e.getCont() + 1);
            } else {
                entities.put(key, new Entity(nodeList.item(i).getTextContent(), 1, tag));
            }
        }
    }
}
