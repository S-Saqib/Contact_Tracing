/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package contact_tracing;

import db.TrajStorage;
import java.io.IOException;
import ds.qtrajtree.TQIndex;
import ds.trajectory.Trajectory;
import io.real.TrajParser;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import query.service.DistanceConverter;
import query.service.ServiceQueryProcessor;
import query.service.TestServiceQuery;
/**
 *
 * @author Saqib
 */
public class Contact_Tracing {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     * @throws java.io.FileNotFoundException
     * @throws java.text.ParseException
     */
    public static void main(String[] args) throws IOException, FileNotFoundException, ParseException {
        
        int numberOfRuns = 100;
        int[] dataSetSizes = {10, 25, 50, 100}; //k
        // 10k, 25k, 50k (default), 100k
        int defaultDataSetSize = 50;    // k
        double[] spatialProximityValues = {1, 2, 4, 10};
        double spatialProximity = 2; // should be around 13 feet for example
        // 1, 2 (default), 4, 10
        String proximityUnit = "m"; // it can be "m", "km", "mile" and "ft"
        long[] temporalProximityValues = {15*60, 30*60, 60*60, 180*60};
        long temporalProximity = 30; // in minutes, may be anything around 5 to 240 for example
        // 15, 30 (default), 60, 180
        temporalProximity *= 60;    // in seconds
        
        int trajLengthId = 1;  // 0 = 1-50, 1 = 51-100 (default), 2 = 101-200, 3 = over 200
        
        int timeWindowInSec = 30*60;
        // using default temporal proximity as the time window used in index
        
        int maxRecursionDepth = 1;
        
        for (int dataSetSize : dataSetSizes){
            String userTrajectoryFilePath = "../Data/sample_" + dataSetSize + "k_traj.txt";
            // parse, preprocess and normalize trajectory data
            TrajParser trajParser = new TrajParser();
            HashMap<String, Trajectory> userTrajectories = trajParser.parseUserTrajectories(userTrajectoryFilePath);
            System.out.println("Trajectory Parsed");
        
            // create an object of TrajStorage to imitate database functionalities
            TrajStorage trajStorage = new TrajStorage(userTrajectories);
            trajStorage.prepareQueryDataset();
            
            // build index on the trajectory data (assuming we have all of it in memory)
            long fromTime = System.nanoTime();
            TQIndex quadTrajTree = new TQIndex(trajStorage, trajParser.getLatCoeff(), trajParser.getLatConst(), trajParser.getLonCoeff(), trajParser.getLonConst(), 
                                trajParser.getMaxLat(), trajParser.getMaxLon(), trajParser.getMinLat(), trajParser.getMinLon(), trajParser.getMinTimeInSec(), timeWindowInSec);
            long toTime = System.nanoTime();
            System.out.println("Index built for " + dataSetSize + "k Trajs, time: " + (toTime-fromTime)/1.0e9);
            
            // show some statistics of the recently built index
            // Statistics stats = new Statistics(quadTrajTree);
            // stats.printStats();

            // show the indexed trajectories visually along with the appropriate q-nodes
            // quadTrajTree.draw();
            
            for (int recDepth = 1; recDepth <= 5; recDepth++){
                for (long tProx : temporalProximityValues){
                    for (double sProx : spatialProximityValues){
                        for (int tLenId = 0; tLenId < 4; tLenId++){
                            if (sProx != spatialProximity && tProx != temporalProximity) continue;
                            if (sProx != spatialProximity && tLenId != trajLengthId) continue;
                            if (sProx != spatialProximity && recDepth != maxRecursionDepth) continue;
                            if (sProx != spatialProximity && dataSetSize != defaultDataSetSize) continue;
                            if (tProx != temporalProximity && tLenId != trajLengthId) continue;
                            if (tProx != temporalProximity && recDepth != maxRecursionDepth) continue;
                            if (tProx != temporalProximity && dataSetSize != defaultDataSetSize) continue;
                            if (tLenId != trajLengthId && recDepth != maxRecursionDepth) continue;
                            if (tLenId != trajLengthId && dataSetSize != defaultDataSetSize) continue;
                            if (recDepth != maxRecursionDepth && dataSetSize != defaultDataSetSize) continue;
                            
                            if (recDepth > 1) numberOfRuns = 10;
                            else numberOfRuns = 100;
                            // calculating spatial proximity
                            DistanceConverter distanceConverter = new DistanceConverter(trajParser.getMaxLon(), trajParser.getMaxLat(),
                                                                                        trajParser.getMinLon(), trajParser.getMinLat());
                            double latProximity = distanceConverter.getLatProximity(sProx, proximityUnit);
                            double lonProximity = distanceConverter.getLonProximity(sProx, proximityUnit);
                            //System.out.println(latProximity + " and " + lonProximity);
                            double t, io, infectedCount;
                            t = io = infectedCount = 0;
                            for (int i=0; i<numberOfRuns; i++){
                                ArrayList <Trajectory> facilityGraph = new ArrayList<Trajectory>();
                                // The following trajectory will be received as input
                                //facilityGraph.add(trajStorage.getTrajectoryById("AAH03JAAQAAAO9VAA/"));
                                //facilityGraph.add(trajStorage.getTrajectoryById(trajStorage.getTrajDataAsList().get(0).getAnonymizedId()));
                                //System.out.println(facilityGraph.get(0).getPointList().size());
                                Trajectory inputTraj = trajStorage.getQueryTrajectory(tLenId);
                                String inputTrajId = new String(inputTraj.getAnonymizedId());
                                int points = inputTraj.getPointList().size();
                                System.out.print(inputTrajId + " " + points);
                                facilityGraph.add(trajStorage.getQueryTrajectory(tLenId));
                                ArrayList<Double> measures = TestServiceQuery.run(trajStorage, quadTrajTree, facilityGraph, latProximity, lonProximity, tProx, recDepth);
                                t += measures.get(0);
                                io += measures.get(1);
                                //ioTraj += measures.get(2);
                                infectedCount += measures.get(2);
                            }
                            t /= numberOfRuns;
                            io /= numberOfRuns;
                            infectedCount /= numberOfRuns;
                            //ioTraj /= numberOfRuns;
                            String pointBucketRange = "";
                            if (tLenId == 0) pointBucketRange = "1-50";
                            else if (tLenId == 1) pointBucketRange = "51-100";
                            else if (tLenId == 2) pointBucketRange = "101-200";
                            else pointBucketRange = "> 200";
                            System.out.println("\nParameters: " + dataSetSize + "k Trajs, " + pointBucketRange + " Points, " + sProx + " Meters, "
                                                + tProx/60 + " Minutes, Upto Level " + recDepth);
                            System.out.println(" Time = " + t + " , IO = " + io + " Avg. Infected = " + infectedCount + "\n");
                        }
                    }
                }
            }
        }
    }
}
