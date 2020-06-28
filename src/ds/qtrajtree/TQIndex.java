package ds.qtrajtree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.util.Assert;
import db.DbInterface;
import db.TrajStorage;
import ds.qtree.Node;
import ds.qtree.NodeType;
import ds.qtree.QuadTree;
import ds.rtree.Rtree;
import ds.trajectory.TrajPoint;
import ds.trajectory.Trajectory;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TQIndex {

    private final QuadTree quadTree;
    
    private final TrajStorage trajStorage;
    
    // trying to maintain a map of trajectories contained in qNode, this is analogous to the block number of the trajectory
    // public Map<Node, ArrayList<Trajectory>> qNodeToTrajsMap;
    // cannot maintain the aforementioned map for scalability
    
    // maintaining a map of number of trajectories contained in qNode, will remove later unless needed
    public Map<Node, Integer> qNodeTrajsCount;
    
    // maintaining a map of trajectory ids contained in qNode, will remove later unless needed
    public Map<Node, ArrayList<String>> qNodeToAnonymizedTrajIdsMap;
    // second level quadtrees mapped to first level quadtree nodes
    public Map<Node, QuadTree> qNodeToNextLevelIndexMap;
    
    //public double latCoeff = 0, latConst = 0, lonCoeff = 0, lonConst = 0;
    // these coefficients and constants may be needed to get back the actual longitudes, latitudes of trajectories later
    private final double latCoeff, latConst, lonCoeff, lonConst;
    private final double maxLat, maxLon, minLat, minLon;
    private final long minTimeInSec;
    private final int timeWindowInSec;
    
    private final DbInterface dbInterface;
    
    public TQIndex(TrajStorage trajStorage, int timeWindowInSec) throws SQLException {
        
        this.trajStorage = trajStorage;
        
        this.latCoeff = trajStorage.getLatCoeff();
        this.latConst = trajStorage.getLatConst();
        this.lonCoeff = trajStorage.getLonCoeff();
        this.lonConst = trajStorage.getLonConst();
        
        this.maxLat = trajStorage.getMaxLat();
        this.maxLon = trajStorage.getMaxLon();
        this.minLat = trajStorage.getMinLat();
        this.minLon = trajStorage.getMinLon();
        
        this.minTimeInSec = trajStorage.getMinTimeInSec();
        
        this.timeWindowInSec = timeWindowInSec;
        
        this.dbInterface = new DbInterface();
        
        qNodeTrajsCount = new HashMap<Node, Integer>();
        qNodeToAnonymizedTrajIdsMap = new HashMap<Node, ArrayList<String>>();
        qNodeToNextLevelIndexMap = new HashMap<Node, QuadTree>();
        
        quadTree = new QuadTree(trajStorage, 0.0, 0.0, 100.0, 100.0, minTimeInSec, timeWindowInSec);    // since trajectories are already normalized in this range
        
        // now read data in chunks and build the first level quadtree
        
        long fromTime = System.currentTimeMillis();
        // String normalizedPointQuery = "SELECT anonymous_id, (point).lat, (point).lng, (point).ts FROM raw_data where ts::date = ?";
        
        final String trajTableName = "normalized_trajectory_day_one";
        // final String trajTableName = "normalized_trajectory";    // 7 days
        
        // resetting if anything from previous run exists
        String resetBlockIdQuery = "update " + trajTableName + " set rtree_block_id = -1";
        PreparedStatement pstmt = dbInterface.getConnection().prepareStatement(resetBlockIdQuery);
        pstmt.executeUpdate();
        
        final String indexName = "rtree_block_id_" + trajTableName + "_idx";
        String dropIdxQuery = "DROP INDEX if exists " + indexName;
        pstmt = dbInterface.getConnection().prepareStatement(dropIdxQuery);
        pstmt.execute();
        
        dbInterface.commit();
        long toTime = System.currentTimeMillis();
        System.out.println("Rtree block ids and its index reset, required time = " + (toTime-fromTime)/1000 + " seconds");
        
        final int fetchSize = 1000;    // small during dev, larger during run
        final int numOfRows = 10000;    // small during dev, to be removed during run
        final int commitAfterTrajs = 1000; // small during dev, larger during run
        
        String normalizedTrajQuery = "select anonymous_id, normalized_points from " + trajTableName + " LIMIT ?";
        pstmt = dbInterface.getConnection().prepareStatement(normalizedTrajQuery);
        
        pstmt.setInt(1, numOfRows);
        pstmt.setFetchSize(fetchSize);
        
        int trajsProcessed = 0;
        
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()){
            String anonymizedId = rs.getString(1);
            Array trajPointArray = rs.getArray(2);
            Object[] trajPointList = (Object[])trajPointArray.getArray();
            int pointCount = 0;
            for(Object trjPtObj: trajPointList){
                TrajPoint trajPoint = new TrajPoint(trjPtObj);
                double normalizedLat = trajPoint.getPointLocation().x;
                double normalizedLon = trajPoint.getPointLocation().y;
                long trajPointTimeInSec = trajPoint.getTimeInSec();
                if (normalizedLat < 0 || normalizedLat > 100 || normalizedLon < 0 || normalizedLon > 100){
                    if (normalizedLat < 0){
                        if (normalizedLat > -1.0e-6){
                            normalizedLat = 0;
                        }
                        else{
                            System.out.println("Point (" + normalizedLat + "," + normalizedLon + ") - " + trajPointTimeInSec + " is out of bounds for lat");
                            continue;
                        }
                    }
                    else if (normalizedLat > 100){
                        if (normalizedLat-100 < 1.0e-6){
                            normalizedLat = 100;
                        }
                        else{
                            System.out.println("Point (" + normalizedLat + "," + normalizedLon + ") - " + trajPointTimeInSec + " is out of bounds for lat");
                            continue;
                        }
                    }
                    else{
                        // ok, no issues with lat
                    }
                    if (normalizedLon < 0){
                        if (normalizedLon > -1.0e-6){
                            normalizedLon = 0;
                        }
                        else{
                            System.out.println("Point (" + normalizedLat + "," + normalizedLon + ") - " + trajPointTimeInSec + " is out of bounds for lon");
                            continue;
                        }
                    }
                    else if (normalizedLon > 100){
                        if (normalizedLon-100 < 1.0e-6){
                            normalizedLon = 100;
                        }
                        else{
                            System.out.println("Point (" + normalizedLat + "," + normalizedLon + ") - " + trajPointTimeInSec + " is out of bounds for lon");
                            continue;
                        }
                    }
                    else{
                        // ok, no issues with lon
                    }
                }
                quadTree.set(normalizedLat, normalizedLon, trajPointTimeInSec, pointCount++, anonymizedId);
            }
            
            trajsProcessed++;
            if (trajsProcessed % commitAfterTrajs == 0){
                trajStorage.getDbInterface().commit();
                toTime = System.currentTimeMillis();
                System.out.println(trajsProcessed + " / " + numOfRows + " trajs processed, required time = " + (toTime-fromTime)/1000 + " seconds");
            }

        }
        trajStorage.getDbInterface().commit();
        toTime = System.currentTimeMillis();
        System.out.println(trajsProcessed + " / " + numOfRows + " trajs processed, required time = " + (toTime-fromTime)/1000 + " seconds");
        
        dbInterface.freeConnection();
        
        toTime = System.currentTimeMillis();
        System.out.println("Quadtree construction time (including data retrieval) : " + (toTime-fromTime)/1000 + " seconds");
        
        // qr-tree
        {
            // assuming zCode starts from 0 (the second argument)
            quadTree.assignZCodesToLeaves(quadTree.getRootNode(), 0);
            // coordinate transformation
            quadTree.transformTrajectories(quadTree.getRootNode());
            trajStorage.getDbInterface().commit();
            
            toTime = System.currentTimeMillis();
            System.out.println("Trajectories transformed, time : " + (toTime-fromTime)/1000 + " seconds");
            // transformed traj envelope computation
            // trajStorage.printTrajectories();

            // grouping in transformed coordinates (QR tree)
            // deal chunkwise and pass to rtree accordingly
            Rtree rTree = new Rtree(trajStorage.getTransformedTrajData());
            
            toTime = System.currentTimeMillis();
            System.out.println("Trajectories grouped, time : " + (toTime-fromTime)/1000 + " seconds");
            
            trajStorage.setTrajIdToDiskBlockIdMap(rTree.getTrajectoryToLeafMapping());
            trajStorage.setDiskBlockIdToTrajIdListMap();
            
            toTime = System.currentTimeMillis();
            System.out.println("Disk blocks tagged to trajs in db table, time : " + (toTime-fromTime)/1000 + " seconds");
            
            // assigning block no. to qNode leaves
            quadTree.tagDiskBlockIdsToNodes(quadTree.getRootNode());
            
            trajStorage.clusterTrajData();
            toTime = System.currentTimeMillis();
            System.out.println("Table containting block id and trajectory data clustered, time : " + (toTime-fromTime)/1000 + " seconds");
            
            // removing redundant temporary information
            trajStorage.clearQNodeToPointListMap();
            
            toTime = System.currentTimeMillis();
            System.out.println("Qnode to points map db table emptied, time : " + (toTime-fromTime)/1000 + " seconds");
        }
        trajStorage.getDbInterface().commit();
        
        toTime = System.currentTimeMillis();
        System.out.println("QR-tree index built, time : " + (toTime-fromTime)/1000 + " seconds");
        System.exit(0);
    }
    
    private void addTrajectories(ArrayList<Trajectory> trajectories) {
        for (Trajectory trajectory : trajectories) {
            Node node = addTrajectory(quadTree.getRootNode(), trajectory);
            
            if (!qNodeToNextLevelIndexMap.containsKey(node)) {
                qNodeToNextLevelIndexMap.put(node, new QuadTree(trajStorage, node.getX(), node.getY(), node.getX() + node.getW(), node.getY() + node.getH(),
                                                                minTimeInSec, timeWindowInSec));
                qNodeToAnonymizedTrajIdsMap.put(node, new ArrayList<String>());
            }
            String anonymizedTrajId = trajectory.getAnonymizedId();
            qNodeToAnonymizedTrajIdsMap.get(node).add(anonymizedTrajId);
        }
    }

    private Node addTrajectory(Node node, Trajectory trajectory) {
        
        if (node.getNodeType() == NodeType.LEAF){
            return node;
        }
        
        Envelope trajEnv = trajectory.getSpatialEnv();
        
        Envelope envNe = getNodeEnvelop(node.getNe());
        if (!envNe.contains(trajEnv) && envNe.intersects(trajEnv)) {
            return node;
        }
        
        Envelope envNw = getNodeEnvelop(node.getNw());
        if (!envNw.contains(trajEnv) && envNw.intersects(trajEnv)) {
            return node;
        }
        
        Envelope envSe = getNodeEnvelop(node.getSe());
        if (!envSe.contains(trajEnv) && envSe.intersects(trajEnv)) {
            return node;
        }
        
        Envelope envSw = getNodeEnvelop(node.getSw());
        if (!envSw.contains(trajEnv) && envSw.intersects(trajEnv)) {
            return node;
        }

        if (envNe.contains(trajEnv)) {
            return addTrajectory(node.getNe(), trajectory);
        }
        if (envNw.contains(trajEnv)) {
            return addTrajectory(node.getNw(), trajectory);
        }
        if (envSe.contains(trajEnv)) {
            return addTrajectory(node.getSe(), trajectory);
        }
        if (envSw.contains(trajEnv)) {
            return addTrajectory(node.getSw(), trajectory);
        }

        System.out.println(trajEnv);

        System.out.println("QuadTrajTree.addTrajectory()");
        Assert.shouldNeverReachHere();

        return null;
    }

    private Envelope getNodeEnvelop(Node node) {
        Envelope nodeEnv = new Envelope();
        nodeEnv.expandToInclude(node.getX(), node.getY());
        nodeEnv.expandToInclude(node.getX() + node.getW(), node.getY() + node.getH());
        return nodeEnv;
    }
    
    public QuadTree getQuadTree() {
        return quadTree;
    }

    public ArrayList<String> getQNodeTrajsId(Node node) {
        ArrayList<String> empty = new ArrayList<String>();
        if (node == null) {
            return empty;
        }
        ArrayList<String> ret = qNodeToAnonymizedTrajIdsMap.get(node);
        ////ArrayList<Integer> ret = null;
        return ret == null ? empty : ret;
    }

    public ArrayList<Trajectory> getQNodeTrajs(Node node) {
        ArrayList<String> retIds = getQNodeTrajsId(node);
        ArrayList<Trajectory> empty = new ArrayList<Trajectory>();
        ArrayList<Trajectory> ret = new ArrayList<Trajectory>();
        for (int i=0; i<retIds.size(); i++){
            ret.add(trajStorage.getTrajectoryById(retIds.get(i)));
        }
        return ret == null ? empty : ret;
    }

    public QuadTree getQNodeQuadTree(Node node) {
        if (qNodeToNextLevelIndexMap.containsKey(node)){
            return qNodeToNextLevelIndexMap.get(node);
        }
        return null;
    }
    
    public int getTotalNodeTraj(Node qNode) {
        if (qNodeTrajsCount.get(qNode) != null) {
            return qNodeTrajsCount.get(qNode);
        }
        return 0;
    }
    
    public double getLatCoeff() {
        return latCoeff;
    }

    public double getLatConst() {
        return latConst;
    }

    public double getLonCoeff() {
        return lonCoeff;
    }

    public double getLonConst() {
        return lonConst;
    }

    public double getMaxLat() {
        return maxLat;
    }

    public double getMaxLon() {
        return maxLon;
    }

    public double getMinLat() {
        return minLat;
    }

    public double getMinLon() {
        return minLon;
    }
    
}
