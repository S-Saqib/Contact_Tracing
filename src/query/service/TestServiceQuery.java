package query.service;

import db.TrajStorage;
import java.util.ArrayList;
import ds.qtrajtree.TQIndex;
import ds.trajectory.TrajPoint;
import ds.trajectory.Trajectory;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;
import org.javatuples.Pair;

public class TestServiceQuery {

    public static HashMap<String, CTQResultTrajData> run(TrajStorage trajStorage, TQIndex quadTrajTree, ArrayList<Trajectory> facilityGraph,
                            double latDisThreshold, double lonDisThreshold, long temporalDisThreshold, int maxRecursionDepth) throws SQLException {
        
        HashMap<String, CTQResultTrajData> responseJsonMap = new HashMap<String, CTQResultTrajData>();
        if (facilityGraph == null || facilityGraph.isEmpty()){
            return responseJsonMap;
        }
        
        for (Trajectory facility : facilityGraph){
            CTQResultTrajData ctqResult = new CTQResultTrajData(0); // exposure level = 0 for query traj
            // assumption for query traj no of exposure = -1, ranks = 0, exposed timestamp = timestamp of first point
            ctqResult.setNoOfExposures(-1);
            ctqResult.setRankByNoOfExposures(0);
            ctqResult.setExposedTimestamp(trajStorage.getTimestamp(facility.getPointList().first().getTimeInSec()));
            ctqResult.setRankByEarliestExposureTimestamp(0);
            TreeSet<TrajPoint> facilityTrajPoints = facility.getPointList();
            for (TrajPoint trajPoint : facilityTrajPoints){
                double lat = trajStorage.denormalizeLat(trajPoint.getPointLocation().x);
                double lon = trajStorage.denormalizeLat(trajPoint.getPointLocation().y);
                long ts = trajPoint.getTimeInSec();
                ctqResult.addPointToAllPoints(new Pair<Long, Pair<Double, Double>>(ts, new Pair<Double, Double>(lat,lon)));
                // exposed points contain all points in case of query trajectory
                ctqResult.addPointToExposedPoints(new Pair<Long, Pair<Double, Double>>(ts, new Pair<Double, Double>(lat,lon)));
            }
            responseJsonMap.put(facility.getAnonymizedId(), ctqResult);
        }
        
        long fromTime = System.nanoTime();
        ServiceQueryProcessor processQuery = new ServiceQueryProcessor(trajStorage, quadTrajTree, latDisThreshold, lonDisThreshold, temporalDisThreshold);
        HashMap<String, TreeSet<TrajPoint>> infectedContacts = new HashMap<String, TreeSet<TrajPoint>>();
        int maxLevel = maxRecursionDepth;
        
        for (int level=1; level<=maxLevel; level++){
            infectedContacts = processQuery.calculateCover(quadTrajTree.getQuadTree(), facilityGraph, infectedContacts, responseJsonMap);
            facilityGraph.clear();

            String[] infectedTrajIdList = new String[infectedContacts.keySet().size()];
            int trajCount = 0;
            for (String infectedTrajId : infectedContacts.keySet()){
                infectedTrajIdList[trajCount++] = infectedTrajId;
            }

            ArrayList <Trajectory> newlyInfectedTrajectories = trajStorage.getTrajectoriesByMultipleIds(infectedTrajIdList);

            trajCount = 0;
            for (HashMap.Entry<String, TreeSet<TrajPoint>> entry : infectedContacts.entrySet()){
                Trajectory newlyInfected = newlyInfectedTrajectories.get(trajCount++);
                Trajectory infectedPortion = new Trajectory();
                infectedPortion.setAnonymizedId(newlyInfected.getAnonymizedId());
                infectedPortion.setUserId(newlyInfected.getUserId());
                infectedPortion.setContactNo(newlyInfected.getContactNo());
                infectedPortion.setPointList(newlyInfected.getPointList());

                // sanity check, but may not enter the if condition
                if (infectedPortion == null || infectedPortion.getPointList() == null || infectedPortion.getPointList().size() == 0) continue;

                infectedPortion.setPointList((TreeSet<TrajPoint>) infectedPortion.getPointList().tailSet(entry.getValue().first()));
                
                facilityGraph.add(infectedPortion);
                
                // insert appropriate items in responseJsonMap
                if (responseJsonMap.get(entry.getKey()).getExposureLevel() == -1){
                    responseJsonMap.get(entry.getKey()).setExposureLevel(level);
                }
                
                if (responseJsonMap.get(entry.getKey()).getExposureLevel() == level){
                    for (TrajPoint responseTrajPoint : entry.getValue()){
                        double lat = trajStorage.denormalizeLat(responseTrajPoint.getPointLocation().x);
                        double lon = trajStorage.denormalizeLat(responseTrajPoint.getPointLocation().y);
                        long ts = responseTrajPoint.getTimeInSec();
                        responseJsonMap.get(entry.getKey()).addPointToExposedPoints(new Pair<Long, Pair<Double, Double>>(ts, new Pair<Double, Double>(lat,lon)));
                    }
                }
            }
        }
        
        for (HashMap.Entry<String, TreeSet<TrajPoint>> entry : infectedContacts.entrySet()){
            responseJsonMap.get(entry.getKey()).setExposedTimestamp(trajStorage.getTimestamp(entry.getValue().first().getTimeInSec()));
            responseJsonMap.get(entry.getKey()).setNoOfExposures(entry.getValue().size());
        }
        
        
        long toTime = System.nanoTime();

        ArrayList <Double> timeIO = new ArrayList<Double>();
        timeIO.add((toTime-fromTime)/1.0e9);
        timeIO.add(processQuery.getBlocksAccessed()*1.0);
        timeIO.add(infectedContacts.size()*1.0);
        System.out.println(infectedContacts.size() + " users infected, runtime = " + timeIO.get(0) + " seconds, blocks accessed = " + timeIO.get(1));
        System.out.println("Infected Users:");
        for (Map.Entry<String, TreeSet<TrajPoint>> entry : infectedContacts.entrySet()){
            System.out.print(entry.getKey() + " ");
        }
        System.out.println("");
        
        return responseJsonMap;
    }
}
