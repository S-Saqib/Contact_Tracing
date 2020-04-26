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
        
        String userTrajectoryFilePath = "../Data/sample_4.txt";
        
        // parse, preprocess and normalize trajectory data
        TrajParser trajParser = new TrajParser();
        HashMap<String, Trajectory> userTrajectories = trajParser.parseUserTrajectories(userTrajectoryFilePath);
        
        // create an object of TrajStorage to imitate database functionalities
        TrajStorage trajStorage = new TrajStorage(userTrajectories);
        
        // build index on the trajectory data (assuming we have all of it in memory)
        TQIndex quadTrajTree = new TQIndex(trajStorage, trajParser.getLatCoeff(), trajParser.getLatConst(), trajParser.getLonCoeff(), trajParser.getLonConst(), 
                                            trajParser.getMaxLat(), trajParser.getMaxLon(), trajParser.getMinLat(), trajParser.getMinLon());
        
        // show some statistics of the recently built index
        Statistics stats = new Statistics(quadTrajTree);
        stats.printStats();
        
        // show the indexed trajectories visually along with the appropriate q-nodes
        quadTrajTree.draw();
        
        // calculating spatial proximity
        double spatialProximity = 1; // in feet, should be around 13 for example
        String proximityUnit = "km"; // it can be "m", "km", "mile" and "ft"
        DistanceConverter distanceConverter = new DistanceConverter(trajParser.getMaxLon(), trajParser.getMaxLat(), trajParser.getMinLon(), trajParser.getMinLat());
        double latProximity = distanceConverter.getLatProximity(spatialProximity, proximityUnit);
        double lonProximity = distanceConverter.getLonProximity(spatialProximity, proximityUnit);
        System.out.println(latProximity + " and " + lonProximity);
        
        long temporalProximity = 10; // in minutes, may be anything around 5 to 240 for example
        temporalProximity *= 60;    // in seconds
        
        ArrayList <Trajectory> facilityGraph = new ArrayList<Trajectory>();
        // The following trajectory will be received as input
        facilityGraph.add(trajStorage.getTrajectoryById("AAH03JACmAAEbrSAqN"));
        // facilityGraph.add(trajStorage.getTrajectoryById(trajStorage.getTrajDataAsList().get(0).getAnonymizedId()));
        //System.out.println(facilityGraph.get(0).getPointList().size());
        TestServiceQuery.run(quadTrajTree, facilityGraph, latProximity, lonProximity, temporalProximity);
    }
    
}
