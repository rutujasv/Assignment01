package com.cs.assignment;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import com.cs.assignment.util.Constants;
import com.cs.assignment.util.CreateDbTable;
import com.cs.assignment.entity.Event;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReadFile {

    private static final Logger logger = Logger.getLogger(ReadFile.class.getName());

    public static void main(String args[]) throws IOException, ParseException {
        logger.info("Reading file :  " + args[0]);
        ArrayList<JSONObject> json = new ArrayList<JSONObject>();
        JSONObject jsonObject;
        String line = null;
        try {
            FileReader fileReader = new FileReader(args[0]);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            Event event;
            Map<String, Event> eventMap = new HashMap<>();

            while ((line = bufferedReader.readLine()) != null) {

                event = new Event();
                jsonObject = (JSONObject) new JSONParser().parse(line);
                json.add(jsonObject);

                event.setId((String) jsonObject.get(Constants.KEY_ID));
                event.setType((String) jsonObject.get(Constants.KEY_TYPE));
                event.setHost((String) jsonObject.get(Constants.KEY_HOST));

                if (Constants.KEY_STATE_STARTED.equals((String) jsonObject.get(Constants.KEY_STATE))) {
                    if (eventMap.get((String) jsonObject.get(Constants.KEY_ID)) != null) {
                        eventMap.get((String) jsonObject.get(Constants.KEY_ID)).setStartTime((String) jsonObject.get(Constants.KEY_TIMESTAMP));
                    } else
                        event.setStartTime((String) jsonObject.get(Constants.KEY_TIMESTAMP));
                } else if (Constants.KEY_STATE_FINISHED.equals((String) jsonObject.get(Constants.KEY_STATE))) {
                    if (eventMap.get((String) jsonObject.get(Constants.KEY_ID)) != null) {
                        eventMap.get((String) jsonObject.get(Constants.KEY_ID)).setEndTime((String) jsonObject.get(Constants.KEY_TIMESTAMP));

                    } else
                        event.setEndTime((String) jsonObject.get(Constants.KEY_TIMESTAMP));
                }

                eventMap.put((String) jsonObject.get(Constants.KEY_ID), event);
                addToDb(eventMap);

            }
            bufferedReader.close();
        } catch (FileNotFoundException ex) {
            logger.log(Level.SEVERE,"Unable to open file '" + args[0] + "'");
        } catch (IOException ex) {
            logger.log(Level.SEVERE,"Error reading file '" + args[0] + "'");
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    private static void addToDb(Map<String,Event> eventMap) {
        logger.info("Adding event to Database");
        eventMap.forEach((key, event) ->{
        if(Integer.parseInt(event.getEndTime()) - Integer.parseInt(event.getStartTime()) > 4){
            logger.info("Event "+ event.getId()+ " has taken longer than 4ms, Raising alert.. ");
            event.setAlert(true);
            event.setDuration((Integer.parseInt(event.getEndTime()) - Integer.parseInt(event.getStartTime())));
            try {
                CreateDbTable.insertData(event);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }});

    }
}
