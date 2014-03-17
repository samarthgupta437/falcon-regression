/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.regression.Entities;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.falcon.regression.core.enumsAndConstants.FEED_TYPE;
import org.apache.falcon.regression.core.generated.dependencies.Frequency;
import org.apache.falcon.regression.core.generated.feed.*;
import org.apache.falcon.regression.core.util.HCatUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hive.hcatalog.api.HCatClient;
import org.joda.time.DateTime;

import javax.xml.bind.JAXBException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class FeedMerlin extends Feed {
  private Feed element;

    public FeedMerlin(String entity) throws JAXBException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
      element = InstanceUtil.getFeedElement(entity);

      Field[] fields = Feed.class.getDeclaredFields();
      for (Field fld : fields) {
        System.out.println("current field: "+fld.getName());
        if("acl".equals(fld.getName()))
          continue;
          PropertyUtils.setProperty(this, fld.getName(),
          PropertyUtils.getProperty(element, fld.getName()));
      }
    }

    /*
    all Merlin specific operations
     */
    public String getTargetCluster() {

      for (Cluster c : getClusters().getCluster()) {
        if (c.getType().equals(ClusterType.TARGET))
          return c.getName();
      }

      return "";
    }

    public void generateData(HCatClient cli, FileSystem fs, boolean isEmpty)throws Exception{
        FEED_TYPE dataType;
        ArrayList<String> dataFolder;
        String ur = element.getTable().getUri();

        if(ur.contains(";")){
            String[] parts = ur.split("#")[1].split(";");
            int len=parts.length;
            dataType=getDataType(len);
        }else {
            dataType=FEED_TYPE.YEARLY;
        }


        String dbName=ur.split("#")[0].split(":")[1];
        String tableName=ur.split("#")[0].split(":")[2];

        String loc = cli.getTable(dbName,tableName).getLocation();
        loc=loc+"/";

        dataFolder = createTestData(fs , dataType, loc, isEmpty);
        HCatUtil.createHCatTestData(cli, fs, dataType, dbName, tableName, dataFolder);
    }

    public void generateData(FileSystem fs, boolean isEmpty)throws Exception{
        FEED_TYPE dataType;
        String pathValue="";
        for (Location location : element.getLocations().getLocation()) {
            if (location.getType().equals(LocationType.DATA)) {
                pathValue=location.getPath();
            }
        }

        String[] parts = pathValue.split("/$");
        int len=parts.length;
        if(len!=2){
            dataType=getDataType(len-1);
        }else {
            dataType=FEED_TYPE.YEARLY;
        }

        String loc = pathValue.substring(0,pathValue.indexOf("$"));
        createTestData(fs, dataType, loc, isEmpty);
    }

    public ArrayList<String> createTestData(FileSystem fs, FEED_TYPE dataType, String loc, boolean isEmpty)throws Exception{
        ArrayList<String> dataFolder = new ArrayList<String>();

        Date start = element.getClusters().getCluster().get(0).getValidity().getStart();
        Format formatter = new SimpleDateFormat("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
        String startDate = formatter.format(start);
        Date end = element.getClusters().getCluster().get(0).getValidity().getEnd();
        String endDate = formatter.format(end);

        DateTime startDateJoda = new DateTime(InstanceUtil.oozieDateToDate(startDate));
        DateTime endDateJoda = new DateTime(InstanceUtil.oozieDateToDate(endDate));

        dataFolder = HadoopUtil.createTestDataInHDFS(fs, Util.getDatesOnEitherSide(startDateJoda,endDateJoda,dataType), loc, isEmpty);
        return dataFolder;
    }


    public FEED_TYPE getDataType(int len){

        if(len==5) {
            return FEED_TYPE.MINUTELY;
        }
        else if(len==4) {
            return FEED_TYPE.HOURLY;
        }
        else if(len==3){
            return FEED_TYPE.DAILY;
        }
        else if(len==2) {
            return FEED_TYPE.MONTHLY;
        }
        return null;
    }

    public String insertRetentionValueInFeed(String feed, String retentionValue)
            throws JAXBException {

        //insert retentionclause
        element.getClusters().getCluster().get(0).getRetention()
                .setLimit(new Frequency(retentionValue));

        for (org.apache.falcon.regression.core.generated.feed.Cluster cluster : element
                .getClusters().getCluster()) {
            cluster.getRetention().setLimit(new Frequency(retentionValue));
        }

        return InstanceUtil.feedElementToString(element);
    }

    public String setTableValue(String feed, String pathValue, String dBName, String tableName) throws Exception {

        element.getTable().setUri("catalog:"+dBName+":"+tableName+"#"+pathValue);
        //set the value
        return InstanceUtil.feedElementToString(element);
    }

}