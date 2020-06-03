package query.service;

import db.TrajStorage;
import java.util.ArrayList;
import ds.qtrajtree.TQIndex;
import ds.trajectory.TrajPoint;
import ds.trajectory.Trajectory;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;

public class TestServiceQuery {

    public static ArrayList<Double> run(TrajStorage trajStorage, TQIndex quadTrajTree, ArrayList<Trajectory> facilityGraph,
                            double latDisThreshold, double lonDisThreshold, long temporalDisThreshold, int maxRecursionDepth) {

        //int numberOfRuns = 10;
        //double naiveTime = 0, zOrderTime = 0;
        //for (int i = 0; i < numberOfRuns; i++) {
            ServiceQueryProcessor processQuery = new ServiceQueryProcessor(trajStorage, quadTrajTree, latDisThreshold, lonDisThreshold, temporalDisThreshold);
            //System.out.println("--Service Query--");
            //System.out.println("Optimal:");
            //double from = System.nanoTime();
            HashMap<String, TreeSet<TrajPoint>> infectedContacts = new HashMap<String, TreeSet<TrajPoint>>();
            // infectedContacts = processQuery.evaluateService(quadTrajTree.getQuadTree().getRootNode(), facilityGraph, infectedContacts);
            HashSet<String> alreadyInfectedIds = new HashSet<String>();
            alreadyInfectedIds.add(facilityGraph.get(0).getAnonymizedId());
            int maxLevel = maxRecursionDepth;
            
            long fromTime = System.nanoTime();
            for (int level=1; level<=maxLevel; level++){
                infectedContacts = processQuery.calculateCover(quadTrajTree.getQuadTree(), facilityGraph, infectedContacts, alreadyInfectedIds);
                facilityGraph.clear();
                for (HashMap.Entry<String, TreeSet<TrajPoint>> entry : infectedContacts.entrySet()){
                    Trajectory newlyInfected = trajStorage.getTrajectoryById(entry.getKey());
                    Trajectory infectedPortion = new Trajectory();
                    infectedPortion.setAnonymizedId(newlyInfected.getAnonymizedId());
                    infectedPortion.setUserId(newlyInfected.getUserId());
                    infectedPortion.setContactNo(newlyInfected.getContactNo());
                    infectedPortion.setPointList(newlyInfected.getPointList());
                    
                    if (infectedPortion == null || infectedPortion.getPointList() == null || infectedPortion.getPointList().size() == 0) continue;
                    infectedPortion.setPointList((TreeSet<TrajPoint>) infectedPortion.getPointList().tailSet(entry.getValue().first()));
                    facilityGraph.add(infectedPortion);
                    alreadyInfectedIds.add(entry.getKey());
                }
            }
            long toTime = System.nanoTime();
            
            System.out.println(" : " + infectedContacts.size());
            ArrayList <Double> timeIO = new ArrayList<Double>();
            timeIO.add((toTime-fromTime)/1.0e9);
            timeIO.add(processQuery.getBlocksAccessed()*1.0);
            //timeIO.add(processQuery.getTrajectoriesAccessed()*1.0);
            timeIO.add(infectedContacts.size()*1.0);
            //processQuery.clearBlocksAccessed();
            return timeIO;
            /*
            for (HashMap.Entry<String, TreeSet<TrajPoint>> entry : infectedContacts.entrySet()){
                System.out.print(entry.getKey() + " : ");
                for (TrajPoint trajPoint : entry.getValue()){
                    System.out.print(trajPoint.toString() + " ");
                }
                System.out.println("");
            }
            */
            //double to = System.nanoTime();
            //System.out.println("Number of routes: " + facilityQuery.size() + "\nNumber of users served = " + (int) serviceValue + "\nTime: " + (to - from) / 1e9 + "s");
            /*
            if (serviceValue < 1){
                i--;
                continue;
            }
            */
            //zOrderTime += (to - from) / 1e9;
            //System.out.println("Brute Force:");
            //from = System.nanoTime();
            //serviceValue = processQuery.evaluateServiceBruteForce(quadTrajTree.getQuadTree().getRootNode(), facilityQuery);
            //to = System.nanoTime();
            //System.out.println("Number of routes: " + facilityQuery.size() + "\nNumber of users served = " + (int) serviceValue
            //        + "\nTime: " + (to - from) / 1e9 + "s");
            //naiveTime += (to - from) / 1e9;
        //}
        //naiveTime /= numberOfRuns;
        //zOrderTime /= numberOfRuns;
        //System.out.println (naiveTime + "\n" + zOrderTime);
    }
    
    public static ArrayList<Double> runQ2R(TrajStorage trajStorage, TQIndex quadTrajTree, ArrayList<Trajectory> facilityGraph,
                            double latDisThreshold, double lonDisThreshold, long temporalDisThreshold, int maxRecursionDepth) {

            ServiceQueryProcessor processQuery = new ServiceQueryProcessor(trajStorage, quadTrajTree, latDisThreshold, lonDisThreshold, temporalDisThreshold);
            HashMap<String, TreeSet<TrajPoint>> infectedContacts = new HashMap<String, TreeSet<TrajPoint>>();
            // infectedContacts = processQuery.evaluateService(quadTrajTree.getQuadTree().getRootNode(), facilityGraph, infectedContacts);
            HashSet<String> alreadyInfectedIds = new HashSet<String>();
            alreadyInfectedIds.add(facilityGraph.get(0).getAnonymizedId());
            int maxLevel = maxRecursionDepth;
            
            long fromTime = System.nanoTime();
            for (int level=1; level<=maxLevel; level++){
                infectedContacts = processQuery.evaluateService(quadTrajTree.getQuadTree().getRootNode(), facilityGraph, infectedContacts, alreadyInfectedIds);
                facilityGraph.clear();
                for (HashMap.Entry<String, TreeSet<TrajPoint>> entry : infectedContacts.entrySet()){
                    Trajectory newlyInfected = trajStorage.getTrajectoryById(entry.getKey());
                    Trajectory infectedPortion = new Trajectory();
                    infectedPortion.setAnonymizedId(newlyInfected.getAnonymizedId());
                    infectedPortion.setUserId(newlyInfected.getUserId());
                    infectedPortion.setContactNo(newlyInfected.getContactNo());
                    infectedPortion.setPointList(newlyInfected.getPointList());
                    
                    if (infectedPortion == null || infectedPortion.getPointList() == null || infectedPortion.getPointList().size() == 0) continue;
                    infectedPortion.setPointList((TreeSet<TrajPoint>) infectedPortion.getPointList().tailSet(entry.getValue().first()));
                    facilityGraph.add(infectedPortion);
                    alreadyInfectedIds.add(entry.getKey());
                }
            }
            long toTime = System.nanoTime();
            
            System.out.println(" : " + infectedContacts.size());
            ArrayList <Double> timeIO = new ArrayList<Double>();
            timeIO.add((toTime-fromTime)/1.0e9);
            timeIO.add(processQuery.getQ2RBlocksAccessed()*1.0);
            //timeIO.add(processQuery.getTrajectoriesAccessed()*1.0);
            timeIO.add(infectedContacts.size()*1.0);
            //processQuery.clearBlocksAccessed();
            return timeIO;
    }
}
