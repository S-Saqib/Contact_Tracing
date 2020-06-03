package ds.trajectory;

import com.vividsolutions.jts.geom.Envelope;
import java.util.TreeSet;

public class Trajectory {
    
    private TreeSet<TrajPoint> pointList;
    private long userId;
    private String anonymizedId;
    private String contactNo;
    private Envelope spatialEnv;

    public Trajectory() {
        pointList = new TreeSet<TrajPoint>(new TrajPointComparator());
        anonymizedId = new String();
        contactNo = null;
        userId = -1;
        spatialEnv = new Envelope();
    }
    
    public Trajectory(String anonymizedId, long userId){
        pointList = new TreeSet<TrajPoint>(new TrajPointComparator());
        this.anonymizedId = anonymizedId;
        this.userId = userId;
        contactNo = null;
        spatialEnv = new Envelope();
    }
    
    public Trajectory(TreeSet<TrajPoint> pointList) {
        this.pointList = pointList;
        anonymizedId = new String();
        contactNo = null;
        userId = -1;
        spatialEnv = new Envelope();
    }
    
    public void setPointList(TreeSet<TrajPoint> pointList) {
        this.pointList = pointList;
    }

    public TreeSet<TrajPoint> getPointList() {
        return pointList;
    }
    
    public String getAnonymizedId() {
        return anonymizedId;
    }

    public void setAnonymizedId(String anonymizedId) {
        this.anonymizedId = anonymizedId;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getContactNo() {
        return contactNo;
    }

    public void setContactNo(String contactNo) {
        this.contactNo = contactNo;
    }
    
    public void addTrajPoint(TrajPoint p){
        if (pointList == null){
            // should not come here
            pointList = new TreeSet<TrajPoint>(new TrajPointComparator());
        }
        pointList.add(p);
    }

    public Envelope getSpatialEnv() {
        return spatialEnv;
    }

    public void setSpatialEnv(Envelope trajSpatialEnv) {
        this.spatialEnv = trajSpatialEnv;
    }
    
    public void setSpatialEnv(){
        if (pointList == null || pointList.isEmpty()) return;
        for(TrajPoint trajPoint : pointList){
            spatialEnv.expandToInclude(trajPoint.getPointLocation());
        }
    }
    
    public boolean contains(TrajPoint p){
        return pointList.contains(p);
    }

    @Override
    public String toString() {
        String trajString = "Trajectory ID = " + userId + " , Anonymized ID = " + anonymizedId + " , Contact No. = " + contactNo + "\n";
        trajString += pointList.toString() + "\n";
        return trajString;
    }
    
    
        
}
