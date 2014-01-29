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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.falcon.regression.core.interfaces;

import com.jcraft.jsch.JSchException;
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.supportClasses.ENTITY_TYPE;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.StringUtils;
import org.apache.oozie.client.OozieClient;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

public abstract class IEntityManagerHelper {

  protected String CLIENT_LOCATION = "src/test/resources/IvoryClient/IvoryCLI.jar";
  protected String BASE_COMMAND = "java -jar " + CLIENT_LOCATION;

  public String getActiveMQ() {
    return activeMQ;
  }

  public String getHadoopLocation() {
    return hadoopLocation;
  }

  public String getHadoopURL() {
    return hadoopURL;
  }

  public String getHostname() {
    return hostname;
  }

  public String getOozieLocation() {
    return oozieLocation;
  }

  public String getOozieURL() {
    return oozieURL;
  }

  public String getPassword() {
    return password;
  }

  public String getStoreLocation() {
    return storeLocation;
  }

  public String getUsername() {
    return username;
  }

  //basic properties
  protected String qaHost;

  public String getQaHost() {
    return qaHost;
  }

  protected String hostname = "";
  protected String username = "";
  protected String password = "";
  protected String hadoopLocation = "";
  protected String hadoopURL = "";
  protected String oozieURL = "";
  protected String oozieLocation = "";
  protected String activeMQ = "";
  protected String storeLocation = "";
  protected String hadoopGetCommand = "";
  protected String envFileName;
  protected String colo;
  protected String allColo;
  protected String serviceStartCmd;
  protected String serviceStopCmd;
  protected String serviceRestartCmd;
  protected String serviceStatusCmd;

  public OozieClient getOozieClient() {
    if (null == this.oozieClient) {
      this.oozieClient = OozieUtil.getClient(this.oozieURL);
    }
    return this.oozieClient;
  }

  protected OozieClient oozieClient;

  public FileSystem getHadoopFS() throws IOException {
    if (null == this.hadoopFS)
      this.hadoopFS = HadoopUtil.getFileSystem(this.hadoopURL);
    return this.hadoopFS;
  }

  protected FileSystem hadoopFS;

  public String getIdentityFile() {
    return identityFile;
  }

  protected String identityFile;

    /*public String getServiceStatusMsg() {
        return serviceStatusMsg;
    }*/

    /*public String getServiceStatusCmd() {
        return serviceStatusCmd;
    }*/

  protected String serviceStatusMsg;

  public String getServiceUser() {
    return serviceUser;
  }

  public String getServiceRestartCmd() {
    return serviceRestartCmd;
  }

  public String getServiceStopCmd() {
    return serviceStopCmd;
  }

  public String getServiceStartCmd() {
    return serviceStartCmd;
  }

  protected String serviceUser;

  public String getEnvFileName() {
    return envFileName;
  }

  public IEntityManagerHelper() {

  }

  public IEntityManagerHelper(String envFileName, String prefix) {
    if ((null == prefix) || prefix.isEmpty()) {
      prefix = "";
    } else {
      prefix += ".";
    }
    System.out.println("envFileName: " + envFileName);
    Properties prop = Util.getPropertiesObj(envFileName);
    this.qaHost = prop.getProperty(prefix + "qa_host");
    this.hostname = prop.getProperty(prefix + "ivory_hostname");
    this.username = prop.getProperty(prefix + "username", System.getProperty("user.name"));
    this.password = prop.getProperty(prefix + "password", "");
    this.hadoopLocation = prop.getProperty(prefix + "hadoop_location");
    this.hadoopURL = prop.getProperty(prefix + "hadoop_url");
    this.oozieURL = prop.getProperty(prefix + "oozie_url");
    this.oozieLocation = prop.getProperty(prefix + "oozie_location");
    this.activeMQ = prop.getProperty(prefix + "activemq_url");
    this.storeLocation = prop.getProperty(prefix + "storeLocation");
    this.hadoopGetCommand =
      hadoopLocation + "  fs -cat hdfs://" + hadoopURL +
        "/projects/ivory/staging/ivory/workflows/process";
    this.envFileName = envFileName;
    this.allColo = "?colo=" + prop.getProperty(prefix + "colo", "*");
    this.colo = (!prop.getProperty(prefix + "colo", "").isEmpty()) ? "?colo=" + prop
      .getProperty(prefix + "colo") : "";
    this.serviceStartCmd = prop.getProperty(prefix + "service_start_cmd", "/etc/init.d/tomcat6 start");
    this.serviceStopCmd = prop.getProperty(prefix + "service_stop_cmd",
      "/etc/init.d/tomcat6 stop");
    this.serviceRestartCmd = prop.getProperty(prefix + "service_restart_cmd",
      "/etc/init.d/tomcat6 restart");
    this.serviceUser = prop.getProperty(prefix + "service_user", null);
    this.serviceStatusMsg = prop.getProperty(prefix + "service_status_msg",
      "Tomcat servlet engine is running with pid");
    this.serviceStatusCmd =
      prop.getProperty(prefix + "service_status_cmd", "/etc/init.d/tomcat6 status");
    this.identityFile = prop.getProperty(prefix + "identityFile",
      System.getProperty(prefix + "user.home") + "/.ssh/id_rsa");
    this.hadoopFS = null;
    this.oozieClient = null;
  }

  public abstract ServiceResponse submitEntity(String url, String data) throws IOException;

  public abstract ServiceResponse submitEntity(Util.URLS url, String data) throws IOException;

    /*public abstract ServiceResponse validateEntity(String url, String data) ;*/

  public abstract ServiceResponse schedule(String url, String data) throws JAXBException, IOException, URISyntaxException;

  public abstract ServiceResponse submitAndSchedule(String url, String data) throws IOException;

  public abstract ServiceResponse submitAndSchedule(URLS url, String data) throws IOException;

  public abstract ServiceResponse delete(String url, String data) throws JAXBException, IOException, URISyntaxException;

  public abstract ServiceResponse suspend(String url, String data) throws JAXBException, IOException, URISyntaxException;

  public abstract ServiceResponse resume(String url, String data) throws JAXBException, IOException, URISyntaxException;

  public abstract ServiceResponse resume(URLS url, String data) throws JAXBException, IOException, URISyntaxException;

  public abstract ServiceResponse getStatus(String url, String data) throws JAXBException, IOException, URISyntaxException;

  public abstract ServiceResponse getStatus(URLS url, String data) throws JAXBException, IOException, URISyntaxException;

  public abstract ServiceResponse getEntityDefinition(String url, String data) throws JAXBException, IOException, URISyntaxException;

  public abstract ServiceResponse getEntityDefinition(URLS url, String data) throws JAXBException, IOException, URISyntaxException;


  public abstract void validateResponse(String response, APIResult.Status expectedResponse,
                                        String filename) throws JAXBException, IOException
    ;

  public abstract ServiceResponse schedule(URLS scheduleUrl, String processData) throws JAXBException, IOException, URISyntaxException;

  public abstract ServiceResponse delete(URLS deleteUrl, String data) throws JAXBException, IOException, URISyntaxException;

  public abstract ServiceResponse suspend(URLS suspendUrl, String data) throws JAXBException, IOException, URISyntaxException;

  public abstract ProcessInstancesResult getRunningInstance(URLS processRuningInstance,
                                                            String name) throws IOException, URISyntaxException;


    /*public abstract ProcessInstancesResult getRunningInstance(String processRuningInstance,
                                                              String name)
    ;*/


  public abstract ProcessInstancesResult getProcessInstanceStatus(
    String readEntityName, String params) throws IOException, URISyntaxException;

  public abstract ProcessInstancesResult getProcessInstanceSuspend(
    String readEntityName, String params) throws IOException, URISyntaxException;

/*    public abstract String writeEntityToFile(String entity) ;

    public abstract String submitEntityViaCLI(String filePath) ;

    public abstract String validateEntityViaCLI(String filePath) ;

    public abstract String submitAndScheduleViaCLI(String filePath) ;

    public abstract String scheduleViaCLI(String filePath) ;

    public abstract String resumeViaCLI(String filePath) ;

    public abstract String getStatusViaCLI(String filePath) ;

    public abstract String getEntityDefinitionViaCLI(String filePath) ;

    public abstract String deleteViaCLI(String filePath) ;

    public abstract String suspendViaCLI(String filePath) ;

    public abstract String updateViaCLI(String processName, String newProcessFilePath)
    ;*/

  public abstract String list() throws IOException, InterruptedException;

  public abstract String getDependencies(String entityName) throws IOException, InterruptedException;

  public abstract List<String> getArchiveInfo() throws IOException, JSchException;

  public abstract List<String> getStoreInfo() throws IOException, JSchException;

  public abstract ServiceResponse update(String oldEntity, String newEntity) throws JAXBException, IOException;

  public abstract ServiceResponse update(String oldEntity,
                                String newEntity,
                                String updateTime
  ) throws IOException, JAXBException;

  public ServiceResponse updateRequestHelper(String oldEntity,
                                            String newEntity,
                                            String updateTime,
                                            String updateUrl) throws JAXBException, IOException {
    String url = this.hostname + updateUrl  + "/" +
      Util.readEntityName(oldEntity);

    if(org.apache.commons.lang.StringUtils.isEmpty(colo))
      return Util.sendRequest(url + "?end=" + updateTime, newEntity);

    return Util.sendRequest(url + colo + "&end=" + updateTime, newEntity);

  }

  public abstract String toString(Object object) throws JAXBException;

/*
    public abstract ProcessInstancesResult getInstanceRerun(String EntityName, String params)
    ;
*/

  public abstract ProcessInstancesResult getProcessInstanceKill(String readEntityName,
                                                                String string) throws IOException, URISyntaxException;

  public abstract ProcessInstancesResult getProcessInstanceRerun(String readEntityName,
                                                                 String string) throws IOException, URISyntaxException
    ;

  public abstract ProcessInstancesResult getProcessInstanceResume(String readEntityName,
                                                                  String string) throws IOException, URISyntaxException
    ;

  public String getColo() {
    return colo;
  }

/*
    public abstract String getProcessInstanceStatusViaCli(String EntityName,
                                                          String start, String end, String colos)
    ;
*/
}
