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
        
        String userTrajectoryFilePath = "../Data/sample_50k_traj.txt";
        // 10k, 25k, 50k (default), 100k
        double spatialProximity = 2; // should be around 13 feet for example
        // 1, 2 (default), 4, 10
        String proximityUnit = "m"; // it can be "m", "km", "mile" and "ft"
        long temporalProximity = 30; // in minutes, may be anything around 5 to 240 for example
        // 15, 30 (default), 60, 180
        temporalProximity *= 60;    // in seconds
        int trajLengthId = 1;   // 0 = 1-25, 1 = 26-50 (default), 2 = 51-75, 3 = 76-100 
        int numberOfRuns = 20;
        
        int timeWindowInSec = 30*60;
        
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
        System.out.println("Index built, time:\n" + (toTime-fromTime)/1.0e9);
        // show some statistics of the recently built index
        // Statistics stats = new Statistics(quadTrajTree);
        // stats.printStats();
        
        // show the indexed trajectories visually along with the appropriate q-nodes
        // quadTrajTree.draw();
        
        // calculating spatial proximity
        DistanceConverter distanceConverter = new DistanceConverter(trajParser.getMaxLon(), trajParser.getMaxLat(), trajParser.getMinLon(), trajParser.getMinLat());
        double latProximity = distanceConverter.getLatProximity(spatialProximity, proximityUnit);
        double lonProximity = distanceConverter.getLonProximity(spatialProximity, proximityUnit);
        //System.out.println(latProximity + " and " + lonProximity);
        
        double t, io;
        t = io = 0;
        for (int i=0; i<numberOfRuns; i++){
            ArrayList <Trajectory> facilityGraph = new ArrayList<Trajectory>();
            // The following trajectory will be received as input
            //facilityGraph.add(trajStorage.getTrajectoryById("AAH03JAAQAAAO9VAA/"));
            //facilityGraph.add(trajStorage.getTrajectoryById(trajStorage.getTrajDataAsList().get(0).getAnonymizedId()));
            //System.out.println(facilityGraph.get(0).getPointList().size());
            facilityGraph.add(trajStorage.getQueryTrajectory(trajLengthId));
            ArrayList<Double> measures = TestServiceQuery.run(trajStorage, quadTrajTree, facilityGraph, latProximity, lonProximity, temporalProximity);
            t += measures.get(0);
            io += measures.get(1);
        }
        t /= numberOfRuns;
        io /= numberOfRuns;
        System.out.println(t + " " + io);
    }
}
