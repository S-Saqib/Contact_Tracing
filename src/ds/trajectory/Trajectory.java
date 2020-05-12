package ds.trajectory;

import java.util.TreeSet;

public class Trajectory {
    
    private TreeSet<TrajPoint> pointList;
    private long userId;
    private String anonymizedId;
    private String contactNo;

    public Trajectory() {
        pointList = new TreeSet<TrajPoint>(new TrajPointComparator());
        anonymizedId = new String();
        contactNo = null;
        userId = -1;
    }
    
    public Trajectory(String anonymizedId, long userId){
        pointList = new TreeSet<TrajPoint>(new TrajPointComparator());
        this.anonymizedId = anonymizedId;
        this.userId = userId;
        contactNo = null;
    }
    
    public Trajectory(TreeSet<TrajPoint> pointList) {
        this.pointList = pointList;
        anonymizedId = new String();
        contactNo = null;
        userId = -1;
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
