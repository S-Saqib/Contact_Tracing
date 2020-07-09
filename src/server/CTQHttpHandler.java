/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package server;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import db.TrajStorage;
import ds.qtrajtree.TQIndex;
import ds.trajectory.Trajectory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import query.service.CTQResultTrajData;
import query.service.DistanceConverter;
import query.service.TestServiceQuery;
/**
 *
 * @author Saqib
 */
public class CTQHttpHandler implements HttpHandler {

    private TrajStorage trajStorage;
    private TQIndex quadTrajTree;
    
    public CTQHttpHandler(TrajStorage trajStorage, TQIndex quadTrajTree){
        this.trajStorage = trajStorage;
        this.quadTrajTree = quadTrajTree;
    }
    
    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        
        System.out.println("Got a request");
        
        HashMap<String, String> requestParamValue = null;

        if ("GET".equals(httpExchange.getRequestMethod())) {
            System.out.println("Request is of GET type");
            requestParamValue = handleGetRequest(httpExchange);
            System.out.println("Get request param = " + requestParamValue.toString());

        } else if ("POST".equals(httpExchange.getRequestMethod())) {
            System.out.println("Request is of POST type");
            requestParamValue = handlePostRequest(httpExchange);
            System.out.println("Post request param = " + requestParamValue.toString());

        } else{
            System.out.println("Request is of unknown type");
        }
        
        HashMap<String, CTQResultTrajData> ctqResult = null;
        try {
            ctqResult = processCTQ(requestParamValue);
        } catch (SQLException ex) {
            Logger.getLogger(CTQHttpHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        handleResponse(httpExchange, ctqResult);

    }

    private HashMap<String, String> handleGetRequest(HttpExchange httpExchange) {
        return null;
    }

    private void handleResponse(HttpExchange httpExchange, HashMap<String, CTQResultTrajData> ctqResult) throws IOException {
        System.out.println("In handleResponse method");
        
        Gson gson = new Gson();
        String responseData = gson.toJson(ctqResult);
        System.out.println("Json response data length = " + responseData.length());
        
        httpExchange.sendResponseHeaders(200, responseData.length());
        OutputStream outputStream = httpExchange.getResponseBody();
        
        outputStream.write(responseData.getBytes());

        outputStream.flush();

        outputStream.close();
        /*
        StringBuilder htmlBuilder = new StringBuilder();

        htmlBuilder.append("<html>").
                append("<body>").
                append("<h1>").
                append("Hello ")
                .append(requestParamValue)
                .append("</h1>")
                .append("</body>")
                .append("</html>");

        // encode HTML content 
        // String htmlResponse = StringEscapeUtils.escapeHtml4(htmlBuilder.toString());
        String htmlResponse = htmlBuilder.toString();
        System.out.println(htmlResponse);
        // this line is a must
        httpExchange.sendResponseHeaders(200, htmlResponse.length());
        
        OutputStream outputStream = httpExchange.getResponseBody();
        
        outputStream.write(htmlResponse.getBytes());

        outputStream.flush();

        outputStream.close();
        */
    }
    
    /*
    @Override
    public void handle(HttpExchange he) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    */
    
    private HashMap<String, String> handlePostRequest(HttpExchange httpExchange) throws IOException {
        // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        
        String query = httpExchange.getRequestURI().getQuery();
        String[] queryParams = query.split("&");
        HashMap<String, String> parameters = new HashMap<String, String>();
        for (String queryItem : queryParams){
            String[] queryItemKeyVal = queryItem.split("=");
            parameters.put(queryItemKeyVal[0], queryItemKeyVal[1]);
        }
        
        return parameters;
    }
    
    HashMap<String, CTQResultTrajData> processCTQ(HashMap<String, String> parameters) throws SQLException{
        double spatialProximity = 2; // should be around 13 feet for example
        String proximityUnit = "m"; // it can be "m", "km", "mile" and "ft"
        long temporalProximity = 30; // in minutes, may be anything around 5 to 240 for example
        temporalProximity *= 60;    // in seconds
        int maxRecursionDepth = 1;
        int rankingMethod = 0;  // 0 denotes earliest exposure, 1 denotes number of exposures
        String [] trajIds = null;
        for (HashMap.Entry<String, String> entry : parameters.entrySet()){
            if (entry.getKey().equals("sp")){
                spatialProximity = Double.parseDouble(entry.getValue());
            }
            else if (entry.getKey().equals("tp")){
                temporalProximity = Long.parseLong(entry.getValue());
                temporalProximity *= 60;
            }
            else if(entry.getKey().equals("el")){
                maxRecursionDepth = Integer.parseInt(entry.getValue());
            }
            else if(entry.getKey().equals("rm")){
                rankingMethod = Integer.parseInt(entry.getValue());
            }
            else if (entry.getKey().equals("tid")){
                trajIds = entry.getValue().split(",");
            }
        }
        DistanceConverter distanceConverter = new DistanceConverter(trajStorage.getMaxLon(), trajStorage.getMaxLat(),
                                                                    trajStorage.getMinLon(), trajStorage.getMinLat());

        double latProximity = distanceConverter.getLatProximity(spatialProximity, proximityUnit);
        double lonProximity = distanceConverter.getLonProximity(spatialProximity, proximityUnit);

        ArrayList <Trajectory> facilityGraph = trajStorage.getTrajectoriesByMultipleIds(trajIds);

        return TestServiceQuery.run(trajStorage, quadTrajTree, facilityGraph, latProximity, lonProximity, temporalProximity, maxRecursionDepth);
    }

}
