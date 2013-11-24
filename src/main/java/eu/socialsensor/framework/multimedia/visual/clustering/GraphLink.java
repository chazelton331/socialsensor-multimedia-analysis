/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.socialsensor.framework.multimedia.visual.clustering;
 
/**
 *
 * @author gpetkos
 */
public class GraphLink {
    public float weight;
    public String id;
    public static int counter;
    
    public GraphLink() {
    }

    public GraphLink(float weight) {
        this.weight = weight;
        counter=counter+1;
        id=counter+"";
    }
    
    
}
