/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package query.service;

import com.vividsolutions.jts.geom.Coordinate;
import db.TrajStorage;
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
import java.util.HashSet;
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
    private TrajStorage trajStorage;
    private long blocksAccessed;

    public ServiceQueryProcessor(TrajStorage trajStorage, TQIndex quadTrajTree, double latDisThreshold, double lonDisThreshold, long temporalDisThreshold) {
        this.quadTrajTree = quadTrajTree;
        this.latDisThreshold = latDisThreshold;
        this.lonDisThreshold = lonDisThreshold;
        this.temporalDisThreshold = temporalDisThreshold;
        this.trajStorage = trajStorage;
        this.blocksAccessed = 0;
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
    
    // the following method deals with inter node trajectories organized hierarchically
    // it traverses from root to leaves of the first level quadtree and processes internode trajectories for each node by calling evaluateNodeTraj method
    // should not be needed for QR-tree
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
    
    // the following method obtains the corresponding quadtree for the inter node trajectories of a node and passes to calculate cover for processing them
    // should not be needed for QR tree as we have a single quadtree
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
    
    // actually calculates the overlaps with facility trajectory
    // should be called directly for QR-tree
    public HashMap <String, TreeSet<TrajPoint>> calculateCover(QuadTree quadTree, ArrayList<Trajectory> facilityQuery, HashMap<String, TreeSet<TrajPoint>> contactInfo) {
        for (Trajectory trajectory : facilityQuery) {
            String infectedAnonymizedId = trajectory.getAnonymizedId();
            for (TrajPoint trajPoint : trajectory.getPointList()) {
                // trajPointCoordinate of a facility point
                Coordinate trajPointCoordinate = trajPoint.getPointLocation();
                double infectedX = trajPointCoordinate.x;
                double infectedY = trajPointCoordinate.y;
                double infectedT = trajPoint.getTimeInSec();
                // taking each point of facility subgraph we are checking against the points of inter node trajectories, indexed in the quadtree
                double xMin = infectedX - latDisThreshold;
                double xMax = infectedX + latDisThreshold;
                double yMin = infectedY - lonDisThreshold;
                double yMax = infectedY + lonDisThreshold;
                
                Node[] relevantNodes = quadTree.searchIntersect(xMin, yMin, xMax, yMax);
                
                // calculating time index for the trajectory points
                ArrayList<Integer> timeBuckets = new ArrayList<Integer>();
                int timeIndexFrom = quadTree.getTimeIndex(trajPoint.getTimeInSec());
                int timeIndexTo = quadTree.getTimeIndex(trajPoint.getTimeInSec() + (long) temporalDisThreshold);
                for (int timeIndex = timeIndexFrom; timeIndex <= timeIndexTo; timeIndex++){
                    timeBuckets.add(timeIndex);
                }
                
                HashSet<Integer> relevantDiskBlocks = new HashSet<Integer>();
                for (Node node : relevantNodes){
                    for (int timeIndex : timeBuckets){
                        ArrayList<Object> mappedDiskBlocks = node.getDiskBlocksByQNodeTimeIndex(timeIndex);
                        if (mappedDiskBlocks == null) continue;
                        for (Object blockId : mappedDiskBlocks){
                            relevantDiskBlocks.add((Integer) blockId);
                        }
                    }
                }
                this.blocksAccessed = relevantDiskBlocks.size();
                
                ArrayList<Trajectory> relevantTrajectories = new ArrayList<Trajectory>();
                // need a map for disk block id to trajectory (the reverse of traj to disk block map
                for (Integer blockId : relevantDiskBlocks){
                    for (String trajId : trajStorage.getTrajIdListByBlockId(blockId)){
                        relevantTrajectories.add(trajStorage.getTrajectoryById(trajId));
                    }
                }
                
                for (Trajectory traj : relevantTrajectories){
                    String checkId = traj.getAnonymizedId();
                    for (TrajPoint point : traj.getPointList()){
                        // checking if the point belongs to the same trajectory, if so, it should be ignored
                        if (checkId.equals(infectedAnonymizedId)){
                            continue;
                        }
                        // spatial matching: checking if eucliean distance is within spatialDistanceThreshold
                        double checkX = point.getPointLocation().x;
                        double checkY = point.getPointLocation().y;
                        // need to calculate geodesic distance here
                        double euclideanDistance = Math.sqrt(Math.pow((infectedX - checkX), 2) + Math.pow((infectedY - checkY), 2));
                        if (euclideanDistance <= (latDisThreshold+lonDisThreshold)/2){
                            double checkT = point.getTimeInSec();
                            // temporal matching: checkT should be in [t, t+temporalDistanceThreshold] window for a contact to be affected
                            if (checkT - infectedT >= 0 && checkT - infectedT <= temporalDisThreshold){
                                if (!contactInfo.containsKey(checkId)){
                                    contactInfo.put((String)checkId, new TreeSet<TrajPoint>(new TrajPointComparator()));
                                }
                                contactInfo.get(checkId).add(point);
                            }
                        }
                    }
                }
            }
        }
        return contactInfo;
    }

    public long getBlocksAccessed() {
        return blocksAccessed;
    }
    
}
