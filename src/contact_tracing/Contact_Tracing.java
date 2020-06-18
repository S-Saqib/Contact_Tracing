/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package contact_tracing;

import com.vividsolutions.jts.util.Assert;
import db.TrajStorage;
import java.io.IOException;
import ds.qtrajtree.TQIndex;
import ds.trajectory.Trajectory;
import io.real.TrajParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.SQLException;
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
    public static void main(String[] args) throws IOException, FileNotFoundException, ParseException, SQLException {
        
        double spatialProximity = 2; // should be around 13 feet for example
        String proximityUnit = "m"; // it can be "m", "km", "mile" and "ft"
        long temporalProximity = 30; // in minutes, may be anything around 5 to 240 for example
        temporalProximity *= 60;    // in seconds
        int maxRecursionDepth = 1;
        
        int timeWindowInSec = 30*60;
        // using default temporal proximity as the time window used in index
        
        /* The following portion is moved to database
        String userTrajectoryFilePath = "../Data/NYC_Multipoint.csv";
        // parse, preprocess and normalize trajectory data
        TrajParser trajParser = new TrajParser();
        // HashMap<String, Trajectory> userTrajectories = trajParser.parseUserTrajectories(userTrajectoryFilePath);
        HashMap<String, Trajectory> userTrajectories = trajParser.parseNYFTrajectories(userTrajectoryFilePath);
        System.out.println("Trajectory Parsed");
        */
        
        // create an object of TrajStorage to imitate database functionalities
        TrajStorage trajStorage = new TrajStorage();
        // trajStorage.prepareQueryDataset();
        // trajStorage.generateRandomTrajIds(numberOfRuns);
        
        /*
        // build index on the trajectory data (assuming we have all of it in memory)
        long fromTime = System.nanoTime();
        TQIndex quadTrajTree = new TQIndex(trajStorage, trajParser.getLatCoeff(), trajParser.getLatConst(), trajParser.getLonCoeff(), trajParser.getLonConst(), 
                            trajParser.getMaxLat(), trajParser.getMaxLon(), trajParser.getMinLat(), trajParser.getMinLon(), trajParser.getMinTimeInSec(), timeWindowInSec);
        long toTime = System.nanoTime();
        System.out.println("Index built for " + userTrajectories.size() + " Trajs, time: " + (toTime-fromTime)/1.0e9);
            
        // show some statistics of the recently built index
        Statistics stats = new Statistics(quadTrajTree);
        stats.printStats();

        // show the indexed trajectories visually along with the appropriate q-nodes
        quadTrajTree.draw();
        
        String queryTrajFilePath = "../Data/NYF_Random_Query_Traj_Ids.txt";
        File userTrajectoryFile = new File(queryTrajFilePath);
        Assert.isTrue(userTrajectoryFile.exists(), "query trajectory file not found");
            
        for (int recDepth = 1; recDepth <= 3; recDepth++){
            
            BufferedReader br = null;
            String line = new String();

            br = new BufferedReader(new FileReader(queryTrajFilePath));

            // calculating spatial proximity
            DistanceConverter distanceConverter = new DistanceConverter(trajParser.getMaxLon(), trajParser.getMaxLat(),
                                                                                trajParser.getMinLon(), trajParser.getMinLat());
            double latProximity = distanceConverter.getLatProximity(spatialProximity, proximityUnit);
            double lonProximity = distanceConverter.getLonProximity(spatialProximity, proximityUnit);
                    
            ArrayList <Trajectory> facilityGraph = new ArrayList<Trajectory>();
            // The following trajectory will be received as input
            //facilityGraph.add(trajStorage.getTrajectoryById("AAH03JAAQAAAO9VAA/"));
            //facilityGraph.add(trajStorage.getTrajectoryById(trajStorage.getTrajDataAsList().get(0).getAnonymizedId()));
            //System.out.println(facilityGraph.get(0).getPointList().size());
            // Trajectory inputTraj = trajStorage.getQueryTrajectory(tLenId);
            
            int queryTrajFileLineNo = 15;
            for (int i=1; i<queryTrajFileLineNo; i++) br.readLine();
            
            
            Trajectory inputTraj = trajStorage.getTrajectoryById(br.readLine());
            // Trajectory inputTraj = trajStorage.getTrajectoryById("AAH03JAAQAAAO9WAFx");
            String inputTrajId = new String(inputTraj.getAnonymizedId());
            int points = inputTraj.getPointList().size();
            System.out.print(inputTrajId + " - " + points);
            facilityGraph.add(inputTraj);
            ArrayList<Double> measures = TestServiceQuery.runQ2R(trajStorage, quadTrajTree, facilityGraph, latProximity, lonProximity, temporalProximity, recDepth);
            // ArrayList<Double> measures = TestServiceQuery.run(trajStorage, quadTrajTree, facilityGraph, latProximity, lonProximity, tProx, recDepth);
            double t = measures.get(0);
            double io = measures.get(1);
            //ioTraj += measures.get(2);
            double infectedCount = measures.get(2);

            System.out.println("\nParameters: " + userTrajectories.size() + " Trajs (NYF dataset), " + spatialProximity + " Meters, " + temporalProximity/60 +
                    " Minutes, Upto Level " + recDepth + "\nTime = " + t + " , IO = " + io + " Avg. Infected = " + infectedCount + "\n");
                
        }
        */
        
    }
    
}
