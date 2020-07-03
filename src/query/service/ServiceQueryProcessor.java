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
import ds.qtree.QuadTree;
import ds.trajectory.TrajPoint;
import ds.trajectory.TrajPointComparator;
import ds.trajectory.Trajectory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;
import org.javatuples.Pair;

/**
 *
 * @author Saqib
 */
public class ServiceQueryProcessor {

    private TQIndex quadTrajTree;
    private double latDisThreshold;
    private double lonDisThreshold;
    private double temporalDisThreshold;
    private final TrajStorage trajStorage;
    private HashSet<Integer> blocksAccessed;

    public ServiceQueryProcessor(TrajStorage trajStorage, TQIndex quadTrajTree, double latDisThreshold, double lonDisThreshold, long temporalDisThreshold) {
        this.quadTrajTree = quadTrajTree;
        this.latDisThreshold = latDisThreshold;
        this.lonDisThreshold = lonDisThreshold;
        this.temporalDisThreshold = temporalDisThreshold;
        this.trajStorage = trajStorage;
        this.blocksAccessed = new HashSet<Integer>();
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
    
    // actually calculates the overlaps with facility trajectory
    // should be called directly for QR-tree
    public HashMap <String, TreeSet<TrajPoint>> calculateCover(QuadTree quadTree, ArrayList<Trajectory> facilityQuery, HashMap<String, TreeSet<TrajPoint>> contactInfo,
                                                                HashMap<String, CTQResultTrajData> responseJsonMap) throws SQLException {
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
                            blocksAccessed.add((Integer) blockId);
                        }
                    }
                }
                
                ArrayList <String> trajIdList = new ArrayList<String>();
                // ArrayList<Trajectory> relevantTrajectories = new ArrayList<Trajectory>();
                // need a map for disk block id to trajectory (the reverse of traj to disk block map
                for (Integer blockId : relevantDiskBlocks){
                    for (String trajId : trajStorage.getTrajIdListByBlockId(blockId)){
                        // relevantTrajectories.add(trajStorage.getTrajectoryById(trajId));
                        // trajectoriesAccessed.add(trajId);
                        trajIdList.add(trajId);
                    }
                }
                String[] trajIdArray = new String[trajIdList.size()];
                ArrayList<Trajectory> relevantTrajectories = trajStorage.getTrajectoriesByMultipleIds(trajIdList.toArray(trajIdArray));
                
                for (Trajectory traj : relevantTrajectories){
                    String checkId = traj.getAnonymizedId();
                    for (TrajPoint point : traj.getPointList()){
                        // checking if the point belongs to the same trajectory, if so, it should be ignored
                        // if (checkId.equals(infectedAnonymizedId)){
                        // checking if the point belongs to a covid positive user given as input to the method, if so, it should be ignored
                        if (infectedAnonymizedId.equals(checkId)){
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
                                    if (!responseJsonMap.containsKey(checkId)){
                                        responseJsonMap.put(checkId, new CTQResultTrajData());
                                        for (TrajPoint responseTrajPoint : traj.getPointList()){
                                            double lat = trajStorage.denormalizeLat(responseTrajPoint.getPointLocation().x);
                                            double lon = trajStorage.denormalizeLat(responseTrajPoint.getPointLocation().y);
                                            long ts = responseTrajPoint.getTimeInSec();
                                            responseJsonMap.get(checkId).addPointToAllPoints(new Pair<Long, Pair<Double, Double>>(ts, new Pair<Double, Double>(lat,lon)));
                                        }
                                    }
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
    
    public int getBlocksAccessed() {
        return blocksAccessed.size();
    }
}
