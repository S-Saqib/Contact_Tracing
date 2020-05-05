/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package query.service;

import com.vividsolutions.jts.geom.Coordinate;
import ds.qtrajtree.TQIndex;
import ds.qtree.Node;
import ds.qtree.NodeType;
import ds.qtree.Point;
import ds.qtree.QuadTree;
import ds.trajectory.TrajPoint;
import ds.trajectory.TrajPointComparator;
import ds.trajectory.Trajectory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;

/**
 *
 * @author Saqib
 */
public class ServiceQueryProcessor {

    private TQIndex quadTrajTree;
    private double latDisThreshold;
    private double lonDisThreshold;
    private double temporalDisThreshold;

    public ServiceQueryProcessor(TQIndex quadTrajTree, double latDisThreshold, double lonDisThreshold, long temporalDisThreshold) {
        this.quadTrajTree = quadTrajTree;
        this.latDisThreshold = latDisThreshold;
        this.lonDisThreshold = lonDisThreshold;
        this.temporalDisThreshold = temporalDisThreshold;
    }
    
    public ServiceQueryProcessor(TQIndex quadTrajTree) {
        this.quadTrajTree = quadTrajTree;
        this.latDisThreshold = 0;
        this.lonDisThreshold = 0;
        this.temporalDisThreshold = 0;
    }

    public double getLatDisThreshold() {
        return latDisThreshold;
    }

    public void setLatDisThreshold(double latDisThreshold) {
        this.latDisThreshold = latDisThreshold;
    }

    public double getLonDisThreshold() {
        return lonDisThreshold;
    }

    public void setLonDisThreshold(double lonDisThreshold) {
        this.lonDisThreshold = lonDisThreshold;
    }

    public double getTemporalDisThreshold() {
        return temporalDisThreshold;
    }

    public void setTemporalDisThreshold(double temporalDisThreshold) {
        this.temporalDisThreshold = temporalDisThreshold;
    }

    public HashMap <String, TreeSet<TrajPoint>> evaluateService(Node qNode, ArrayList<Trajectory> facilityQuery, HashMap <String, TreeSet<TrajPoint>> contactInfo) {
        if (facilityQuery == null || facilityQuery.isEmpty() || qNode.getNodeType() == NodeType.EMPTY) {
            return null;
        }
        
        HashMap <String, TreeSet<TrajPoint>> newContactInfo = evaluateNodeTraj(qNode, facilityQuery, contactInfo);
        
        if (newContactInfo != null) {
            for (HashMap.Entry<String, TreeSet<TrajPoint>> entry : newContactInfo.entrySet()) {
                String trajId = entry.getKey();
                TreeSet<TrajPoint> newContactPoints = entry.getValue();
                if (!contactInfo.containsKey(trajId)){
                    contactInfo.put(trajId, newContactPoints);
                }
                else{
                    for (TrajPoint trajPoint: newContactPoints) {
                        contactInfo.get(trajId).add(trajPoint);
                    }
                }
            }
        }
        
        if (qNode.getNodeType() != NodeType.LEAF){
            Node[] qChildren = new Node[4];
            qChildren[0] = qNode.getNe();
            qChildren[1] = qNode.getSe();
            qChildren[2] = qNode.getSw();
            qChildren[3] = qNode.getNw();
            newContactInfo = null;
            for (int k = 0; k < 4; k++) {
                ArrayList<Trajectory> querySubgraphs = clipGraph(qChildren[k], facilityQuery);
                newContactInfo = evaluateService(qChildren[k], querySubgraphs, contactInfo);
                if (newContactInfo != null) {
                    for (HashMap.Entry<String, TreeSet<TrajPoint>> entry : newContactInfo.entrySet()) {
                        String trajId = entry.getKey();
                        TreeSet<TrajPoint> newContactPoints = entry.getValue();
                        if (!contactInfo.containsKey(trajId)){
                            contactInfo.put(trajId, newContactPoints);
                        }
                        else{
                            for (TrajPoint trajPoint: newContactPoints) {
                                contactInfo.get(trajId).add(trajPoint);
                            }
                        }
                    }
                }
            }
        }
        return contactInfo;
    }
    
    private ArrayList<Trajectory> clipGraph(Node node, ArrayList<Trajectory> facilityQuery) {
        ArrayList<Trajectory> clippedSubgraphs = new ArrayList<Trajectory>();
        for (Trajectory trajectory : facilityQuery){
            Trajectory clippedFacility = new Trajectory(trajectory.getAnonymizedId(), trajectory.getUserId());
            for (TrajPoint trajPoint : trajectory.getPointList()){
                if (containsExtended(node, trajPoint)){
                    clippedFacility.addTrajPoint(trajPoint);
                }
            }
            clippedSubgraphs.add(clippedFacility);
        }
        return clippedSubgraphs;
    }
    
    boolean containsExtended(Node qNode, TrajPoint trajPoint) {
        Coordinate coord = trajPoint.getPointLocation();
        // checking whether an extended qNode contains a point
        double minX = qNode.getX() - latDisThreshold;
        double minY = qNode.getY() - lonDisThreshold;
        double maxX = minX + qNode.getW() + 2 * latDisThreshold;
        double maxY = minY + qNode.getH() + 2 * lonDisThreshold;
        if (coord.x < minX || coord.y < minY || coord.x > maxX || coord.y > maxY) {
            return false;
        }
        return true;
    }
    
    private HashMap <String, TreeSet<TrajPoint>> evaluateNodeTraj(Node qNode, ArrayList<Trajectory> facilityQuery, HashMap<String, TreeSet<TrajPoint>> contactInfo) {
        if (facilityQuery == null || facilityQuery.isEmpty()) {
            return null;
        }
        QuadTree interNodeQuadTree = quadTrajTree.getQNodeQuadTree(qNode);
        if (interNodeQuadTree == null || interNodeQuadTree.isEmpty()) {
            return null;
        }
        return calculateCover(interNodeQuadTree, facilityQuery, contactInfo);
    }
    
    private HashMap <String, TreeSet<TrajPoint>> calculateCover(QuadTree interNodeQuadTree, ArrayList<Trajectory> facilityQuery, HashMap<String, TreeSet<TrajPoint>> contactInfo) {
        for (Trajectory trajectory : facilityQuery) {
            String anonymizedId = trajectory.getAnonymizedId();
            for (TrajPoint trajPoint : trajectory.getPointList()) {
                // trajPointCoordinate of a facility point
                Coordinate trajPointCoordinate = trajPoint.getPointLocation();
                double x = trajPointCoordinate.x;
                double y = trajPointCoordinate.y;
                double t = trajPoint.getTimeInSec();
                // taking each point of facility subgraph we are checking against the points of inter node trajectories, indexed in the quadtree
                double xMin = x - latDisThreshold;
                double xMax = x + latDisThreshold;
                double yMin = y - lonDisThreshold;
                double yMax = y + lonDisThreshold;
                Node[] relevantNodes = interNodeQuadTree.searchIntersect(xMin, yMin, xMax, yMax);
                for (Node node : relevantNodes){
                    ArrayList<Point> relevantPoints = node.getPoints();
                    for (Point point: relevantPoints){
                        // checking if the point belongs to the same trajectory, if so, it should be ignored
                        if (((String)point.getTraj_id()).equals(anonymizedId)){
                            continue;
                        }
                        // spatial matching: checking if eucliean distance is within spatialDistanceThreshold
                        double checkX = point.getX();
                        double checkY = point.getY();
                        // need to calculate geodesic distance here
                        double euclideanDistance = Math.sqrt(Math.pow((x - checkX), 2) + Math.pow((y - checkY), 2));
                        if (euclideanDistance <= (latDisThreshold+lonDisThreshold)/2){
                            double checkT = point.getTimeInSec();
                            // temporal matching: checkT should be in [t, t+temporalDistanceThreshold] window for a contact to be affected
                            if (checkT - t >= 0 && checkT - t <= temporalDisThreshold){
                                String infectedTrajId = (String)point.getTraj_id();
                                if (!contactInfo.containsKey(infectedTrajId)){
                                    contactInfo.put((String)point.getTraj_id(), new TreeSet<TrajPoint>(new TrajPointComparator()));
                                }
                                contactInfo.get(infectedTrajId).add(trajPoint);
                            }
                        }
                    }
                }
            }
        }
        return contactInfo;
    }
}
