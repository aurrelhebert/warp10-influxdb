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

package io.warp10.riakts;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
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
 * Function used to load GTS from RIAK db
 *
 */
public class LOADFROMRIAK implements WarpScriptRawJavaFunction {

 
  /**
   * Method to execute when LOADFROMRIAK is called
   * Need 4 elements on top of the stack
   * url riak url
   * port riak port
   * colums is a Map indicating position in riak table of each type of column, and eventually the new column name
   * queryText query to compute on Riak-ts
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
    Object port = stack.pop();
    Object url = stack.pop();
    
    //
    // Check params validity
    //
    
    if (!(url instanceof String)) {
      throw new WarpScriptJavaFunctionException("url must be a String");
      //throw new WarpScriptJavaFunctionException();
    }
    
    if (!(port instanceof Long)) {
      throw new WarpScriptJavaFunctionException("port must be a long");
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
    // Get GTS from Riak server
    //
    
    try {
      Collection<GeoTimeSerie> gts = getGTSfromRiakTS((String) queryText, (Map) colums, ((Number) port).intValue(), (String) url);
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
    } catch (UnknownHostException | ExecutionException | InterruptedException e) {
      // TODO Auto-generated catch block
      throw new WarpScriptJavaFunctionException("Riak exception" + e.getMessage() + e.getStackTrace().toString());
      //throw new WarpScriptJavaFunctionException();
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
   * Method to parse riak-ts data into GTS
   * @param queryText
   * @param columns
   * @param riakDBPort
   * @param riakDBUrl
   * @return
   * @throws WarpScriptJavaFunctionException
   * @throws UnknownHostException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public Collection<GeoTimeSerie> getGTSfromRiakTS(String queryText, Map<String,Object> columns, int riakDBPort, String riakDBUrl) throws WarpScriptJavaFunctionException, UnknownHostException, ExecutionException, InterruptedException {

    //
    // Connect to the Riak Client
    //
    
    RiakClient riakts = RiakClient.newClient(riakDBPort, riakDBUrl);
    
    if (null != riakts) {
      //
      // Get query result in Riak-ts
      //
      
      Query queryb = new Query.Builder(queryText).build();
      QueryResult queryResult = riakts.execute(queryb);
      
      
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
      // Map result initialize
      //
      Map<String, GeoTimeSerie> mapGTS = new HashMap<String, GeoTimeSerie>();
      
      //
      // Analyse all results lines
      //
      
      int count = queryResult.getRowsCount();
      //System.out.println(count);
      List<Row> rows = queryResult.getRowsCopy();
      StringBuffer result = new StringBuffer();
      for (Row row : rows) {
        //System.out.println(row.getCellsCopy().toString());
        List<Cell> cells = row.getCellsCopy();
        
        //
        // Get timestamp value
        //
        
        Long timestamp = null;
        Object tagPosTs = columns.get("timestamp");
        int posTs = getPosition(tagPosTs);
        Cell timestampCell = cells.get(posTs);
        if(timestampCell==null) {
          throw new WarpScriptJavaFunctionException("Null timestamp");
          //throw new WarpScriptJavaFunctionException();
        } else {
          timestamp = timestampCell.getTimestamp() * Constants.TIME_UNITS_PER_MS;
        }
        
        //
        // Get tags/Labels
        //
        
        Map<String,String> labels = new HashMap<String, String>();
        if(columns.containsKey("labels")) {
          Map tag = (Map) columns.get("labels");
          for (Object tagPos : tag.keySet()) {
            int pos = getPosition(tagPos);
            Cell tagCell = cells.get(pos);
            if(null != tagCell) {
              labels.put(tag.get(tagPos).toString(),getValue(tagCell).toString());
            }
          }
        }
        
        //
        // Get latitude
        //
        Double latitude = null;
        if (columns.containsKey("latitude")) {
          Object tagPos = columns.get("latitude");
          int posLat = getPosition(tagPos);
          Cell latCell = cells.get(posLat);
          if(null != latCell) {
            if (latCell.hasDouble()) {
              latitude = latCell.getDouble();
            }
            if (latCell.hasLong()) {
              Long tmp = latCell.getLong();
              latitude = tmp.doubleValue();
            }
          }
        }
        
        //
        // Get longitude
        //
        Double longitude = null;
        if (columns.containsKey("longitude")) {
          Object tagPos = columns.get("longitude");
          int pos = getPosition(tagPos);
          Cell longCell = cells.get(pos);
          if(null != longCell) {
            if (longCell.hasDouble()) {
              longitude = longCell.getDouble();
            }
            if (longCell.hasLong()) {
              Long tmp = longCell.getLong();
              longitude = tmp.doubleValue();
            }
          }
        }
        
        //
        // Convert position to GEOXP point
        //
        
        long location = GeoTimeSerie.NO_LOCATION;
        if (null != longitude && null != latitude) {
          location = GeoXPLib.toGeoXPPoint(latitude, longitude);
        }
        
        //
        // Get elevation
        //
        Long elevation = GeoTimeSerie.NO_ELEVATION;
        if (columns.containsKey("elevation")) {
          Object tagPos = columns.get("elevation");
          int pos = getPosition(tagPos);
          Cell elevCell = cells.get(pos);
          if(null != elevCell) {
            if (elevCell.hasDouble()) {
              Double tmp = elevCell.getDouble();
              elevation = elevCell.getLong();
            }
            if (elevCell.hasLong()) {
              elevation = elevCell.getLong();
            }
          }
        }
        
  
        //
        // Add points to GTS
        //
        
        Map<Integer, String> className = (Map<Integer, String>) columns.get("class");
        
        //
        // For each class set
        //
        
        for (Object classPos : className.keySet()) {
          GeoTimeSerie currentGts = null;
          String name = className.get(classPos) + labels.toString();
          
          //
          // If exists, load builded GTS
          //
          
          if (mapGTS.containsKey(name)) {
            currentGts = mapGTS.get(name);
          } else {
            
            //
            // Otherwise initialize a GTS with current class name and labels
            //
            
            currentGts = new GeoTimeSerie();
            currentGts.setLabels(labels);
            currentGts.setName(className.get(classPos));
          }
          
          //
          // Get current point value
          //

          int pos = getPosition(classPos);
          Cell classCell = cells.get(pos);
          Object value = null;
          if(null != classCell ) {
            value = getValue(classCell);
          }
          
          // 
          // Add current value to the GTS
          //
          
          GTSHelper.setValue(currentGts, timestamp, location, elevation, value, true);
          
          //
          // Save GTS in a Map
          //
          
          mapGTS.put(name,currentGts);
        }
        
      }
      riakts.shutdown();
      return mapGTS.values();
    }
    else {
      return new ArrayList<>();
    }
  }

  /**
   * Method to get riak Cell value according to current object type
   * @param cell
   * @return
   */
  private Object getValue(Cell cell) {
    Object object = null;
    if(cell.hasBoolean()) {
      object = cell.getBoolean();
    }
    if(cell.hasDouble()) {
      object = cell.getDouble();
    }
    if(cell.hasLong()) {
      object = cell.getLong();
    }
    if(cell.hasVarcharValue()) {
      object = cell.getVarcharAsUTF8String();
    }
    if(cell.hasTimestamp()) {
      object = cell.getTimestamp();
    }
    return object;
  }
  
  /**
   * Method to get the position in riak table of a specific column
   * @param tagPos
   * @return
   * @throws WarpScriptJavaFunctionException
   */
  private int getPosition(Object tagPos) throws WarpScriptJavaFunctionException {
    int pos;
    if (tagPos instanceof Number) {
      pos = ((Number) tagPos).intValue();
    } else if (tagPos instanceof String) {
      pos = Integer.parseInt( (String) tagPos);
    } else {
      throw new WarpScriptJavaFunctionException("Position isn't a number");
    }
    return pos;
  }
}
