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
import org.apache.falcon.regression.core.generated.feed.*;
import org.apache.falcon.regression.core.util.HCatUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hive.hcatalog.api.HCatClient;

import javax.xml.bind.JAXBException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class FeedMerlin extends org.apache.falcon.regression.core.generated
  .feed.Feed {
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

    public void generateData(HCatClient cli, FileSystem fs){
        String dataType="";
        String loc="";
        ArrayList<String> dataFolder;
        String ur = element.getTable().getUri();
        if(ur.indexOf(";")!= -1){
            String[] parts = ur.split("#")[1].split(";");
            int len=parts.length;
            if(len==5) dataType="minutely";
            else if(len==4) dataType="hourly";
            else if(len==3) dataType="daily";
            else if(len==2) dataType="monthly";
        }else dataType="yearly";

        String dbName=ur.split("#")[0].split(":")[1];
        String tableName=ur.split("#")[0].split(":")[2];
        try{
            loc = cli.getTable(dbName,tableName).getLocation();
            loc=loc+"/";

            dataFolder=createData(fs, dataType, loc);
            HCatUtil.createHCatTestData(cli, fs, dataType, dbName, tableName, dataFolder, loc);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void generateData(FileSystem fs){
        String dataType="";
        String pathValue="";
        String loc = "";
        for (Location location : element.getLocations().getLocation()) {
            if (location.getType().equals(LocationType.DATA)) {
                pathValue=location.getPath();
            }
        }

        if(pathValue.contains("${MONTH}")){
            String[] parts = pathValue.split("YEAR")[1].split("/");
            int len=parts.length;
            if(len==5) dataType="minutely";
            else if(len==4) dataType="hourly";
            else if(len==3) dataType="daily";
            else if(len==2) dataType="monthly";
        }else dataType="yearly";

        loc = pathValue.substring(0,pathValue.indexOf("$"));
        createData(fs, dataType, loc);
    }

    public ArrayList<String> createData(FileSystem fs, String dataType, String loc){
        ArrayList<String> dataFolder = new ArrayList<String>();
        try{
            if (dataType.equalsIgnoreCase("daily")) {

                dataFolder = HadoopUtil.createTestDataInHDFS(fs, Util.getDailyDatesOnEitherSide(36, 0), loc);
            } else if (dataType.equalsIgnoreCase("yearly")) {
                dataFolder = HadoopUtil.createTestDataInHDFS(fs,Util.getYearlyDatesOnEitherSide(10, 0), loc );
            } else if (dataType.equalsIgnoreCase("monthly")) {
                dataFolder = HadoopUtil.createTestDataInHDFS(fs,Util.getMonthlyDatesOnEitherSide(30, 0), loc );
            } else if (dataType.equalsIgnoreCase("hourly")) {
                dataFolder = HadoopUtil.createTestDataInHDFS(fs,Util.getHourlyDatesOnEitherSide(40, 0), loc );
            } else if (dataType.equalsIgnoreCase("minutely")) {
                dataFolder = HadoopUtil.createTestDataInHDFS(fs,Util.getMinuteDatesOnEitherSide(20, 0), loc );
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return dataFolder;

    }
}
