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

public class TestServiceQuery {

    public static ArrayList<Double> run(TrajStorage trajStorage, TQIndex quadTrajTree, ArrayList<Trajectory> facilityGraph,
                            double latDisThreshold, double lonDisThreshold, long temporalDisThreshold, int maxRecursionDepth, int rankingMethod) throws SQLException {

        
        long fromTime = System.nanoTime();
        ServiceQueryProcessor processQuery = new ServiceQueryProcessor(trajStorage, quadTrajTree, latDisThreshold, lonDisThreshold, temporalDisThreshold);
        HashMap<String, TreeSet<TrajPoint>> infectedContacts = new HashMap<String, TreeSet<TrajPoint>>();
        HashSet<String> alreadyInfectedIds = new HashSet<String>();
        alreadyInfectedIds.add(facilityGraph.get(0).getAnonymizedId());
        int maxLevel = maxRecursionDepth;

        for (int level=1; level<=maxLevel; level++){
            infectedContacts = processQuery.calculateCover(quadTrajTree.getQuadTree(), facilityGraph, infectedContacts, alreadyInfectedIds);
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
                alreadyInfectedIds.add(entry.getKey());
            }
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
        return timeIO;
    }
}
