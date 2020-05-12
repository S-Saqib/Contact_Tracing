package ds.transformed_trajectory;

import java.util.ArrayList;

import java.util.TreeSet;

public class TransformedTrajectory {
    
    private TreeSet<TransformedTrajPoint> transformedPointList;
    private long userId;
    private String anonymizedId;
    private String contactNo;

    public TransformedTrajectory() {
        transformedPointList = new TreeSet<TransformedTrajPoint>(new TransformedTrajPointComparator());
        anonymizedId = new String();
        contactNo = null;
        userId = -1;
    }
    
    public TransformedTrajectory(String anonymizedId, long userId){
        transformedPointList = new TreeSet<TransformedTrajPoint>(new TransformedTrajPointComparator());
        this.anonymizedId = anonymizedId;
        this.userId = userId;
        contactNo = null;
    }
    
    public TransformedTrajectory(TreeSet<TransformedTrajPoint> pointList) {
        this.transformedPointList = pointList;
        anonymizedId = new String();
        contactNo = null;
        userId = -1;
    }
    
    public void setTransformedPointList(TreeSet<TransformedTrajPoint> transformedPointList) {
        this.transformedPointList = transformedPointList;
    }

    public TreeSet<TransformedTrajPoint> getTransformedPointList() {
        return this.transformedPointList;
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
    
    public void addTransformedTrajPoint(TransformedTrajPoint p){
        if (transformedPointList == null){
            // should not come here
            transformedPointList = new TreeSet<TransformedTrajPoint>(new TransformedTrajPointComparator());
        }
        transformedPointList.add(p);
    }
    
    public boolean contains(TransformedTrajPoint p){
        return transformedPointList.contains(p);
    }

    @Override
    public String toString() {
        String trajString = "(Transformed) Trajectory ID = " + userId + " , Anonymized ID = " + anonymizedId + " , Contact No. = " + contactNo + "\n";
        trajString += transformedPointList.toString() + "\n";
        return trajString;
    }
    
    
        
}
