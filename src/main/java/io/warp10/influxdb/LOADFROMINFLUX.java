//
//   Copyright 2016  Cityzen Data
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.influxdb;

import java.net.UnknownHostException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;

import com.geoxp.GeoXPLib;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.WarpScriptJavaFunctionException;
import io.warp10.warp.sdk.WarpScriptRawJavaFunction;
import io.warp10.continuum.store.Constants;

/**
 * Function used to load GTS from Influx db
 *
 */
public class LOADFROMINFLUX implements WarpScriptRawJavaFunction {

 
  /**
   * Method to execute when LOADFROMINFLUX is called
   * Need 4 elements on top of the stack
   * url InfluxDB url
   * port InfluxDB port
   * colums is a Map indicating position in InfluxDB table of each type of column, and the new column name
   * columns can also contains column name in InfluxDB with null value
   * queryText query to compute on InfluxDB
   */
  public List<Object> apply(List<Object> arg0) throws WarpScriptJavaFunctionException {
    
    //
    // Get current stack
    //
    
    WarpScriptStack stack =(WarpScriptStack) arg0.get(0);
    
    //
    // Get params
    //
    
    Object queryText = stack.pop();
    Object colums = stack.pop();
    Object dbName = stack.pop();
    Object password = stack.pop();
    Object user = stack.pop();
    Object url = stack.pop();
    
    //
    // Check params validity
    //

    if (!(url instanceof String)) {
      throw new WarpScriptJavaFunctionException("url must be a String");
      //throw new WarpScriptJavaFunctionException();
    }
    
    if (!(user instanceof String)) {
      throw new WarpScriptJavaFunctionException("user must be a String");
      //throw new WarpScriptJavaFunctionException();
    }
    
    if (!(password instanceof String)) {
      throw new WarpScriptJavaFunctionException("password must be a String");
      //throw new WarpScriptJavaFunctionException();
    }
    
    if (!(dbName instanceof String)) {
      throw new WarpScriptJavaFunctionException("dbName must be a String");
      //throw new WarpScriptJavaFunctionException();
    }
    
    if (!(colums instanceof Map)) {
      throw new WarpScriptJavaFunctionException("columns must be a Map");
      //throw new WarpScriptJavaFunctionException();
    }
    
    if (!(queryText instanceof String)) {
      throw new WarpScriptJavaFunctionException("querytext must be a String");
      //throw new WarpScriptJavaFunctionException();
    }

    //
    // Get GTS from Influx database
    //
    
    try {
      Collection<GeoTimeSerie> gts = getGTSfromInfluxDB((String) queryText, (Map) colums, (String) dbName, (String) url, (String) user, (String) password);
      //stack.push(gts);
      arg0.remove(0);
      List<GeoTimeSerie> liSeries = new ArrayList<GeoTimeSerie>();

      //
      // Convert the collection to an array to put it on stack
      //
      
      for (GeoTimeSerie geoTimeSerie : gts) {
        liSeries.add(geoTimeSerie);
      }

      stack.push(liSeries);
      arg0.add(stack);
    } catch (WarpScriptException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new WarpScriptJavaFunctionException("Stack exception" + e.getMessage() + e.getStackTrace().toString());
      //throw new WarpScriptJavaFunctionException();
    }
    return arg0;
  }

  //
  // Not necessary here
  //
  public int argDepth() {
    return 0;
  }

  //
  // Not necessary here
  //
  public boolean isProtected() {
    // TODO Auto-generated method stub
    return false;
  }

  /**
   * Method to parse InfluxDB data into GTS
   * @param queryText
   * @param columns
   * @param Database
   * @param InfluxUrl
   * @param Influx password
   * @param Influx user
   * @return Collection of GTS
   * @throws WarpScriptJavaFunctionException
   */
  public Collection<GeoTimeSerie> getGTSfromInfluxDB(String queryText, Map<String,Object> columns, String dbName, String influxDBUrl, String user, String password) throws WarpScriptJavaFunctionException {
    
    //
    // Check if Map isValid
    //
    
    if (!columns.containsKey("timestamp")) {
      throw new WarpScriptJavaFunctionException("No timestamp field");
      //throw new WarpScriptJavaFunctionException();
    }
    
    if (!columns.containsKey("class")) {
      throw new WarpScriptJavaFunctionException("No class field");
      //throw new WarpScriptJavaFunctionException();
    }

    if (columns.containsKey("latitude") && !columns.containsKey("longitude") 
        || !columns.containsKey("latitude") && columns.containsKey("longitude") ) {
      throw new WarpScriptJavaFunctionException("Can't have a latitude without a longitude field");
      //throw new WarpScriptJavaFunctionException();
    }
    
    //
    // Open connection with the InfluxDB server
    //
    
    InfluxDB influxDB = InfluxDBFactory.connect(influxDBUrl, user, password);
    
    //
    // Get the result of the query to the specify database
    //
    
    if (null == influxDB) {
      throw new WarpScriptJavaFunctionException("Connection with influxDB failed");
    }
    
    Query query = new Query(queryText, dbName);
    QueryResult result = influxDB.query(query);
    
    if (null == result) {
      throw new WarpScriptJavaFunctionException("InfluxDB query failed");
    }
    
    //
    // Map result initialize
    //
    Map<String, GeoTimeSerie> mapGTS = new HashMap<String, GeoTimeSerie>();

    //
    // Analyse all results lines
    //

    List<Result> results = result.getResults();

    for (Result curResult : results) {
      List<Series> series = curResult.getSeries();

      // 
      // Run throw all result series
      //

      for (Series serie : series) {

        //
        // Get each column names
        //

        List<String> keys = serie.getColumns();

        //
        // Get timestamp position
        // if null take column named time
        // 

        int posTimeStamp;
        if (columns.get("timestamp") == null) {
          try {
            posTimeStamp = getPosition("time", keys);
          } catch (Exception e) {
            throw new WarpScriptJavaFunctionException(e.getMessage());
          }
        } else {
          try {
            posTimeStamp = getPosition(columns.get("timestamp"), keys);
          } catch (Exception e) {
            throw new WarpScriptJavaFunctionException(e.getMessage());
          }
        }

        //
        // Get latitude, longitude and elevation position column
        //
        Integer posLatitude = null, posLongitude = null, posElevation = null;
        if (columns.containsKey("latitude") 
            && columns.containsKey("longitude")) {
          try {
            posLatitude = getPosition(columns.get("latitude"), keys);
          } catch (Exception e) {
            throw new WarpScriptJavaFunctionException(e.getMessage());
          }
          try {
            posLongitude = getPosition(columns.get("longitude"), keys);
          } catch (Exception e) {
            throw new WarpScriptJavaFunctionException(e.getMessage());
          }
        }

        if (columns.containsKey("elevation")) {
          try {
            posElevation = getPosition(columns.get("elevation"), keys);
          } catch (Exception e) {
            throw new WarpScriptJavaFunctionException(e.getMessage());
          }
        }

        //
        // Get labels position columns
        //

        Map<Integer, String> posLabels = new HashMap<Integer, String>(); 

        if (columns.containsKey("labels")) {
          Map tag = (Map) columns.get("labels");
          for (Object tagPos : tag.keySet()) {
            int pos;
            try {
              pos = getPosition(tagPos,keys);
            } catch (Exception e) {
              throw new WarpScriptJavaFunctionException(e.getMessage());
            }
            String value;
            if(tag.get(tagPos) == null) {
              value = tagPos.toString();
            } else {
              value = tag.get(tagPos).toString();
            }
            posLabels.put(pos, value);
          }
        }

        //
        // Get class position columns
        //

        Map<Integer, String> posClass = new HashMap<Integer, String>(); 
        Map className = (Map) columns.get("class");
        for (Object classPos : className.keySet()) {
          int pos;
          try {
            pos = getPosition(classPos,keys);
          } catch (Exception e) {
            throw new WarpScriptJavaFunctionException(e.getMessage());
          }
          String value;
          if(className.get(classPos) == null) {
            value = classPos.toString();
          } else {
            value = className.get(classPos).toString();
          }
          posClass.put(pos, value);
        }

        List<List<Object>> values = serie.getValues();

        //
        // for each values in series
        //

        for (List<Object> list : values) {

          //
          // Compute current labels
          //

          Map<String, String> labels = new HashMap<String, String>();
          for (Integer pos : posLabels.keySet()) {
            if (null!=list.get(pos)) {
              labels.put(posLabels.get(pos),list.get(pos).toString());
            }
          }

          //
          // For each class to load
          //

          for (Integer pos : posClass.keySet()) {

            //
            // Get className
            // 

            String currclassName = posClass.get(pos) + labels.toString();

            //
            // Load or create GTS to add currents values
            //

            GeoTimeSerie currentGTS;
            if(mapGTS.containsKey(currclassName)) {
              currentGTS = mapGTS.get(currclassName);
            } else {
              currentGTS = new GeoTimeSerie();
              currentGTS.setName(posClass.get(pos));
              currentGTS.setLabels(labels);
            }

            //
            // Get timestamp value and convert it to long
            //

            ZonedDateTime zdt = ZonedDateTime.parse(list.get(posTimeStamp).toString());
            long ts = zdt.getLong(ChronoField.INSTANT_SECONDS) * Constants.TIME_UNITS_PER_S + zdt.getLong(ChronoField.NANO_OF_SECOND) / (1000000L / Constants.TIME_UNITS_PER_MS);
            
            //
            // Get location with longitude and latitude values (Double)
            //

            long location = GeoTimeSerie.NO_LOCATION;

            Double latitude = null, longitude = null;

            if (null != posLatitude && null != posLongitude)
            {

              Object curLatitude = getValue(list.get(posLatitude).toString());
              if (curLatitude instanceof Double ) {
                latitude = (Double) curLatitude;
              }

              Object curLongitude= getValue(list.get(posLongitude).toString());
              if (curLongitude instanceof Double) {
                longitude = (Double) curLongitude;
              }
            }

            if (null != longitude && null != latitude) {
              location = GeoXPLib.toGeoXPPoint(latitude, longitude);
            }

            Long elevation = GeoTimeSerie.NO_ELEVATION;

            if (null != posElevation)
            {
              Object curLongitude= getValue(list.get(posElevation).toString());
              if (curLongitude instanceof Double) {
                Double tmp = (Double) curLongitude;
                elevation = Math.round(tmp);
              }
              if (curLongitude instanceof Long) {
                elevation = (Long) curLongitude;
              }
            }

            Object currentValue = null;
            if ( null != list.get(pos) ) {
              currentValue = getValue(list.get(pos).toString());
            }

            GTSHelper.setValue(currentGTS, ts, location, elevation, currentValue, true);
            //double elevation = (Double) list.get(posElevation);
            mapGTS.put(currclassName, currentGTS);
          }
        }
      }
    }
    return mapGTS.values();  
  }
  
  /**
   * Method to get InfluxDB value with correct type
   * @param cell
   * @return
   */
  private Object getValue(String value) {
    try {
      return parse(value); 
    } catch (Exception e) {
      if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
        return Boolean.valueOf(value);
      }else {
        return value;
      }
    }
  }

  /**
   * Method to parse String to get a Number
   * @param value
   * @return
   */
  private Number parse(String str) throws NumberFormatException {
    Number number = null;
    try {
      number = Double.parseDouble(str);
    } catch(NumberFormatException e) {
      try {
        number = Float.parseFloat(str);
      } catch(NumberFormatException e1) {
        try {
          number = Long.parseLong(str);
        } catch(NumberFormatException e2) {
          try {
            number = Integer.parseInt(str);
          } catch(NumberFormatException e3) {
            throw e3;
          }       
        }       
      }       
    }
    return number;
  }

  /**
   * Method to get the position in InfluxDB table of a specific column
   * @param tagPos
   * @return
   * @throws WarpScriptJavaFunctionException
   */
  private int getPosition(Object tagPos, List<String> keys) throws Exception {
    int pos;
    if (tagPos instanceof Number) {
      pos = ((Number) tagPos).intValue();
    } else if (tagPos instanceof String) {
      if(keys.contains(tagPos)) {
        pos = keys.indexOf(tagPos);
      } else {
        pos = Integer.parseInt( (String) tagPos);
      }
    } else {
      throw new Exception("Position isn't a number");
    }
    return pos;
  }

}
