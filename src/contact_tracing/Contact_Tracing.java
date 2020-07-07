/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package contact_tracing;

import com.sun.net.httpserver.HttpServer;
import com.vividsolutions.jts.util.Assert;
import db.TrajStorage;
import java.io.IOException;
import ds.qtrajtree.TQIndex;
import ds.trajectory.Trajectory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import query.service.DistanceConverter;
import query.service.ServiceQueryProcessor;
import query.service.TestServiceQuery;
import server.CTQHttpHandler;
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
        
        // move the following values (index params and db params) to a text file hyperparam.txt so that jar does not have to be rebuilt for making some changes
        // index params
        int qNodePointCapacity = 128;
        int timeWindowInSec = 30*60;
        int rTreeBlockTrajCapacity = 4;
        // using default temporal proximity as the time window used in index
        
        // db params
        String dbLocation = "ec2-3-132-194-145.us-east-2.compute.amazonaws.com";
        String trajTableName = "normalized_trajectory_day_one";
        int fetchSize = 10000;    // small during dev, larger during run
        int numOfRows = 200;    // small during dev, removed during run
        int commitAfterTrajs = 10000;    // small during dev, larger during run
        int useInMemoryQNodePointsMap = 1;
        
        String hyperparamFilePath = "../hyperparam.txt";
        File hyperparamFile = new File(hyperparamFilePath);
        if (hyperparamFile.exists()){
            BufferedReader br = new BufferedReader(new FileReader(hyperparamFile));
            br.readLine();  // ignore the first line which contains explanation
            
            qNodePointCapacity = Integer.parseInt(br.readLine());
            timeWindowInSec = Integer.parseInt(br.readLine());
            rTreeBlockTrajCapacity = Integer.parseInt(br.readLine());
            
            dbLocation = br.readLine();
            trajTableName = br.readLine();
            
            fetchSize = Integer.parseInt(br.readLine());
            numOfRows = Integer.parseInt(br.readLine());
            commitAfterTrajs = Integer.parseInt(br.readLine());
            useInMemoryQNodePointsMap = Integer.parseInt(br.readLine());
            
            System.out.println("Loaded hyper parameters from text file");
        }
        else{
            System.out.println("Continuing with default hyper parameters, as specified in code");
        }
        
        long fromTime = System.nanoTime();
        // create an object of TrajStorage to imitate database functionalities
        TrajStorage trajStorage = new TrajStorage(trajTableName, fetchSize, dbLocation, useInMemoryQNodePointsMap);
        
        TQIndex quadTrajTree = new TQIndex(trajStorage, dbLocation, trajTableName, fetchSize, numOfRows, commitAfterTrajs,
                                            qNodePointCapacity, timeWindowInSec, rTreeBlockTrajCapacity);
        long toTime = System.nanoTime();
        System.out.println("Index built for " + trajStorage.getTrajCount() + " Trajs, time: " + (toTime-fromTime)/1.0e9);
        System.out.println("Now the program will wait for input in an infinite loop.\n"
                + "Give spatial proximity in meter, followed by temporal proximity in minutes, followed by recursion depth, "
                + "followed by ranking method, followed by comma separated trajectory ids (without space/tab/newline)");
        Scanner sc = new Scanner(System.in);
        int rankingMethod;  // 0 denotes earliest exposure, 1 denotes number of exposure
        String [] trajIds;
        
        String ipAddr = "localhost";
        int port = 10001;
        int backlog = 0;
        int threadPoolSize = 10;
        String serverConfigFilePath = "../server_config.txt";
        File serverConfigFile = new File(serverConfigFilePath);
        if (serverConfigFile.exists()){
            BufferedReader br = new BufferedReader(new FileReader(serverConfigFile));
            br.readLine();  // ignore the first line which contains explanation
            
            ipAddr = br.readLine();
            port = Integer.parseInt(br.readLine());
            backlog = Integer.parseInt(br.readLine());
            threadPoolSize = Integer.parseInt(br.readLine());
            
            System.out.println("Loaded server parameters from config file");
        }
        
        HttpServer server = HttpServer.create(new InetSocketAddress(ipAddr, port), backlog);
        server.createContext("/ctq", new CTQHttpHandler(trajStorage, quadTrajTree));
        
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor)Executors.newFixedThreadPool(threadPoolSize);
        server.setExecutor(threadPoolExecutor);
        
        server.start();
        System.out.println("Server started at " + server.getAddress());
        
        /*
        while(true){
            // expect spatial proximity in meters
            spatialProximity = sc.nextDouble();
            // followed by temporal proximity in minutes
            temporalProximity = sc.nextLong();
            temporalProximity *= 60;
            // followed by max recursion depth
            maxRecursionDepth = sc.nextInt();
            // followed by rankingMethod (0: earliest exposure, 1: number of exposure)
            rankingMethod = sc.nextInt();
            // followed by covid-19 positive user ids (anonymized ids)
            trajIds = sc.next().split(",");
            
            DistanceConverter distanceConverter = new DistanceConverter(trajStorage.getMaxLon(), trajStorage.getMaxLat(),
                                                                        trajStorage.getMinLon(), trajStorage.getMinLat());
            
            double latProximity = distanceConverter.getLatProximity(spatialProximity, proximityUnit);
            double lonProximity = distanceConverter.getLonProximity(spatialProximity, proximityUnit);
            
            ArrayList <Trajectory> facilityGraph = trajStorage.getTrajectoriesByMultipleIds(trajIds);
            
            TestServiceQuery.run(trajStorage, quadTrajTree, facilityGraph, latProximity, lonProximity, temporalProximity, maxRecursionDepth, rankingMethod);
        }
    */
    }
}
