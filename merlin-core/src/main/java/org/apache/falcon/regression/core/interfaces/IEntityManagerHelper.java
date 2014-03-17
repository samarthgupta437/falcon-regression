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
import org.apache.falcon.regression.core.response.InstancesSummaryResult;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.oozie.client.AuthOozieClient;

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

    public String getClusterReadonly() {
        return clusterReadonly;
    }

    public String getClusterWrite() {
        return clusterWrite;
    }

    public String getHostname() {
        return hostname;
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

    public String getHCatEndpoint() {return hcatEndpoint; }



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
    protected String clusterReadonly = "";
    protected String clusterWrite = "";
    private String oozieURL = "";
    protected String activeMQ = "";
    protected String storeLocation = "";
    protected String hadoopGetCommand = "";
    protected String envFileName;
    protected String colo;
    protected String allColo;
    protected String coloName;
    protected String serviceStartCmd;
    protected String serviceStopCmd;
    protected String serviceRestartCmd;
    protected String serviceStatusCmd;
    protected String hcatEndpoint = "";

    public String getNamenodePrincipal() {
        return namenodePrincipal;
    }

    public String getHiveMetaStorePrincipal() {
        return hiveMetaStorePrincipal;
    }

    protected String namenodePrincipal;
    protected String hiveMetaStorePrincipal;

    public AuthOozieClient getOozieClient() {
        if (null == this.oozieClient) {
            this.oozieClient = OozieUtil.getClient(this.oozieURL);
        }
        return this.oozieClient;
    }

    protected AuthOozieClient oozieClient;

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
        this.hcatEndpoint = prop.getProperty(prefix + "hcat_endpoint");
        this.clusterReadonly = prop.getProperty(prefix + "cluster_readonly");
        this.clusterWrite = prop.getProperty(prefix + "cluster_write");
        this.oozieURL = prop.getProperty(prefix + "oozie_url");
        this.activeMQ = prop.getProperty(prefix + "activemq_url");
        this.storeLocation = prop.getProperty(prefix + "storeLocation");
        this.hadoopGetCommand =
                hadoopLocation + "  fs -cat hdfs://" + hadoopURL +
                        "/projects/ivory/staging/ivory/workflows/process";
        this.envFileName = envFileName;
        this.allColo = "?colo=" + prop.getProperty(prefix + "colo", "*");
        this.colo = (!prop.getProperty(prefix + "colo", "").isEmpty()) ? "?colo=" + prop
                .getProperty(prefix + "colo") : "";
        this.coloName = this.colo.contains("=") ? this.colo.split("=")[1] : "";
        this.serviceStartCmd =
                prop.getProperty(prefix + "service_start_cmd", "/etc/init.d/tomcat6 start");
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
                System.getProperty("user.home") + "/.ssh/id_rsa");
        this.hadoopFS = null;
        this.oozieClient = null;
        this.namenodePrincipal = prop.getProperty(prefix + "namenode.kerberos.principal", "none");
        this.hiveMetaStorePrincipal = prop.getProperty(prefix + "hive.metastore.kerberos" +
                ".principal", "none");
    }

    public ServiceResponse submitEntity(String url, String data)
            throws IOException, URISyntaxException, AuthenticationException {
        return submitEntity(url, data, null);
    }
    public abstract ServiceResponse submitEntity(String url, String data, String user)
    throws IOException, URISyntaxException, AuthenticationException;

    public ServiceResponse submitEntity(URLS url, String data)
            throws IOException, URISyntaxException, AuthenticationException {
        return submitEntity(url, data, null);
    }
    public abstract ServiceResponse submitEntity(URLS url, String data, String user)
    throws IOException, URISyntaxException, AuthenticationException;

    public ServiceResponse schedule(String url, String data)
            throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        return schedule(url, data, null);
    }

    public abstract ServiceResponse schedule(String url, String data, String user)
    throws JAXBException, IOException, URISyntaxException, AuthenticationException;

    public ServiceResponse submitAndSchedule(String url, String data)
            throws IOException, URISyntaxException, AuthenticationException {
        return submitAndSchedule(url, data, null);
    }
    public abstract ServiceResponse submitAndSchedule(String url, String data, String user)
    throws IOException, URISyntaxException, AuthenticationException;

    public ServiceResponse submitAndSchedule(URLS url, String data)
            throws IOException, URISyntaxException, AuthenticationException {
        return submitAndSchedule(url, data, null);
    }
    public abstract ServiceResponse submitAndSchedule(URLS url, String data, String user)
    throws IOException, URISyntaxException, AuthenticationException;

    public ServiceResponse delete(String url, String data)
            throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        return delete(url, data, null);
    }
    public abstract ServiceResponse delete(String url, String data, String user)
    throws JAXBException, IOException, URISyntaxException, AuthenticationException;

    public ServiceResponse suspend(String url, String data)
            throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        return suspend(url, data, null);
    }
    public abstract ServiceResponse suspend(String url, String data, String user)
    throws JAXBException, IOException, URISyntaxException, AuthenticationException;

    public ServiceResponse resume(String url, String data)
            throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        return resume(url, data, null);
    }
    public abstract ServiceResponse resume(String url, String data, String user)
    throws JAXBException, IOException, URISyntaxException, AuthenticationException;

    public ServiceResponse resume(URLS url, String data)
            throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        return resume(url, data, null);
    }
    public abstract ServiceResponse resume(URLS url, String data, String user)
    throws JAXBException, IOException, URISyntaxException, AuthenticationException;

    public ServiceResponse getStatus(String url, String data)
            throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        return getStatus(url, data, null);
    }
    public abstract ServiceResponse getStatus(String url, String data, String user)
    throws JAXBException, IOException, URISyntaxException, AuthenticationException;

    public ServiceResponse getStatus(URLS url, String data)
            throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        return getStatus(url, data, null);
    }
    public abstract ServiceResponse getStatus(URLS url, String data, String user)
    throws JAXBException, IOException, URISyntaxException, AuthenticationException;

    public ServiceResponse getEntityDefinition(String url, String data)
            throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        return getEntityDefinition(url, data, null);
    }
    public abstract ServiceResponse getEntityDefinition(String url, String data, String user)
    throws JAXBException, IOException, URISyntaxException, AuthenticationException;

    public ServiceResponse getEntityDefinition(URLS url, String data)
            throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        return getEntityDefinition(url, data, null);
    }
    public abstract ServiceResponse getEntityDefinition(URLS url, String data, String user)
    throws JAXBException, IOException, URISyntaxException, AuthenticationException;


    public abstract void validateResponse(String response, APIResult.Status expectedResponse,
                                          String filename) throws JAXBException, IOException
            ;

    public ServiceResponse schedule(URLS scheduleUrl, String processData)
            throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        return schedule(scheduleUrl, processData, null);
    }
    public abstract ServiceResponse schedule(URLS scheduleUrl, String processData, String user)
    throws JAXBException, IOException, URISyntaxException, AuthenticationException;

    public ServiceResponse delete(URLS deleteUrl, String data)
            throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        return delete(deleteUrl, data, null);
    }
    public abstract ServiceResponse delete(URLS deleteUrl, String data, String user)
    throws JAXBException, IOException, URISyntaxException, AuthenticationException;

    public ServiceResponse suspend(URLS suspendUrl, String data)
            throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        return suspend(suspendUrl, data, null);
    }
    public abstract ServiceResponse suspend(URLS suspendUrl, String data, String user)
    throws JAXBException, IOException, URISyntaxException, AuthenticationException;

    public ProcessInstancesResult getRunningInstance(URLS processRunningInstance, String name)
            throws IOException, URISyntaxException, AuthenticationException {
        return getRunningInstance(processRunningInstance, name, null);
    }

    public abstract ProcessInstancesResult getRunningInstance(URLS processRunningInstance,
                                                              String name, String user)
    throws IOException, URISyntaxException, AuthenticationException;

    public ProcessInstancesResult getProcessInstanceStatus(
            String readEntityName, String params)
            throws IOException, URISyntaxException, AuthenticationException {
        return getProcessInstanceStatus(readEntityName, params, null);
    }

    public abstract ProcessInstancesResult getProcessInstanceStatus(
            String readEntityName, String params, String user)
    throws IOException, URISyntaxException, AuthenticationException;

    public ProcessInstancesResult getProcessInstanceSuspend(
            String readEntityName, String params)
            throws IOException, URISyntaxException, AuthenticationException {
        return getProcessInstanceSuspend(readEntityName, params, null);

    }

    public abstract ProcessInstancesResult getProcessInstanceSuspend(
            String readEntityName, String params, String user)
    throws IOException, URISyntaxException, AuthenticationException;

    public abstract String list() throws IOException, InterruptedException;

    public abstract String getDependencies(String entityName)
    throws IOException, InterruptedException;

    public abstract List<String> getArchiveInfo() throws IOException, JSchException;

    public abstract List<String> getStoreInfo() throws IOException, JSchException;

    public ServiceResponse update(String oldEntity, String newEntity)
    throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        return update(oldEntity, newEntity, null);
    }

    public ServiceResponse update(String oldEntity,
                                           String newEntity,
                                           String updateTime) throws IOException, JAXBException, URISyntaxException, AuthenticationException {
        return update(oldEntity, newEntity, updateTime, null);
    }
    public abstract ServiceResponse update(String oldEntity,
                                           String newEntity,
                                           String updateTime,
                                           String user) throws IOException, JAXBException, URISyntaxException, AuthenticationException;

    public ServiceResponse updateRequestHelper(String oldEntity,
                                               String newEntity,
                                               String updateTime,
                                               String updateUrl, String user)
    throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        String url = this.hostname + updateUrl + "/" +
                Util.readEntityName(oldEntity);

        if (org.apache.commons.lang.StringUtils.isEmpty(colo))
            return Util.sendRequest(url + "?effective=" + updateTime,
              newEntity);

        return Util.sendRequest(url + colo + "&effective=" + updateTime,
          newEntity);
    }

    public abstract String toString(Object object) throws JAXBException;

    public ProcessInstancesResult getProcessInstanceKill(String readEntityName, String params)
            throws IOException, URISyntaxException, AuthenticationException {
        return getProcessInstanceKill(readEntityName, params, null);
    }
    public abstract ProcessInstancesResult getProcessInstanceKill(String readEntityName,
                                                                  String string, String user)
    throws IOException, URISyntaxException, AuthenticationException;

    public ProcessInstancesResult getProcessInstanceRerun(String EntityName, String params)
            throws IOException, URISyntaxException, AuthenticationException {
        return getProcessInstanceRerun(EntityName, params, null);
    }
    public abstract ProcessInstancesResult getProcessInstanceRerun(String readEntityName,
                                                                   String string, String user)
    throws IOException, URISyntaxException, AuthenticationException
            ;

    public ProcessInstancesResult getProcessInstanceResume(String EntityName, String params)
            throws IOException, URISyntaxException, AuthenticationException {
        return getProcessInstanceResume(EntityName, params, null);
    }
    public abstract ProcessInstancesResult getProcessInstanceResume(String readEntityName, String string, String user)
    throws IOException, URISyntaxException, AuthenticationException
            ;

  public abstract InstancesSummaryResult getInstanceSummary(String readEntityName,
                                       String string) throws IOException, AuthenticationException,
    URISyntaxException;

  public String getColo() {
    return colo;
  }

  public String getColoName(){ return coloName; }
}
