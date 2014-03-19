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
package org.apache.falcon.regression.core.helpers;

import com.jcraft.jsch.JSchException;
import org.apache.falcon.regression.core.generated.cluster.Cluster;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.InstancesSummaryResult;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.Util;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.List;

public class ClusterEntityHelperImpl extends IEntityManagerHelper {


    private static Logger logger = Logger.getLogger(ClusterEntityHelperImpl.class);

    public ClusterEntityHelperImpl() {

    }

    public ClusterEntityHelperImpl(String envFileName, String prefix)  {
        super(envFileName, prefix);
    }

    public ServiceResponse delete(String url, String data, String user)
    throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        //throw new UnsupportedOperationException("Not supported yet.");
        url += "/cluster/" + Util.readClusterName(data) + colo;
        return Util.sendRequest(url, "delete", user);
    }

    public ServiceResponse getEntityDefinition(String url, String data, String user) throws JAXBException,
    IOException, URISyntaxException, AuthenticationException {
        url += "/cluster/" + Util.readClusterName(data);
        return Util.sendRequest(url, "get", user);
    }

    public ServiceResponse getStatus(String url, String data, String user)  {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public ServiceResponse getStatus(Util.URLS url, String data, String user)  {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public ServiceResponse resume(String url, String data, String user)  {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public ServiceResponse schedule(String url, String data, String user)  {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public ServiceResponse submitAndSchedule(String url, String data, String user)  {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public ServiceResponse submitAndSchedule(Util.URLS url, String data, String user)  {
        return null;
    }

    public ServiceResponse submitEntity(String url, String data, String user)
    throws IOException, URISyntaxException, AuthenticationException {
        //throw new UnsupportedOperationException("Not supported yet.");
        logger.info("Submitting cluster: \n" + Util.prettyPrintXml(data));
        url += "/cluster" + colo;
        return Util.sendRequest(url, "post", data, user);
    }

    public ServiceResponse suspend(String url, String data, String user)  {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public ServiceResponse suspend(Util.URLS url, String data, String user)  {
        throw new UnsupportedOperationException("Not supported yet.");
    }

/*
    public ServiceResponse validateEntity(String url, String data)  {

        return Util.sendPostRequest(url, data);
    }
*/

    public void validateResponse(String response, APIResult.Status expectedResponse,
                                 String filename)  {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ServiceResponse submitEntity(Util.URLS url, String data, String user)
    throws IOException, URISyntaxException, AuthenticationException {
        return submitEntity(this.hostname + url.getValue(), data, user);
    }

    @Override
    public ServiceResponse schedule(Util.URLS scheduleUrl, String processData, String user)
     {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ServiceResponse delete(Util.URLS deleteUrl, String data, String user)
    throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        // TODO Auto-generated method stub
        return delete(this.hostname + deleteUrl.getValue(), data, user);
    }

    @Override
    public ServiceResponse resume(Util.URLS url, String data, String user)  {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProcessInstancesResult getRunningInstance(
            Util.URLS processRunningInstance, String name, String user)  {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProcessInstancesResult getProcessInstanceStatus(
            String readEntityName, String params, String user) {
        // TODO Auto-generated method stub
        return null;
    }


    public ProcessInstancesResult getProcessInstanceSuspend(
            String readEntityName, String params, String user) {
        // TODO Auto-generated method stub
        return null;
    }

    /*public String writeEntityToFile(String entity)  {
        File file = new File("/tmp/" + Util.readClusterName(entity) + ".xml");
        BufferedWriter bf = new BufferedWriter(new FileWriter(file));
        bf.write(entity);
        bf.close();
        return "/tmp/" + Util.readClusterName(entity) + ".xml";
    }*/

    /*@Override
    public String submitEntityViaCLI(String filePath)  {

        //System.out.println(BASE_COMMAND+ " entity -submit -url "+this.hostname+" -type cluster
        // -file "+filePath);
        return Util.executeCommandGetOutput(
                BASE_COMMAND + " entity -submit -url " + this.hostname + " -type cluster -file " +
                        filePath);
    }*/

    /*@Override
    public String validateEntityViaCLI(String entityName)  {

        return Util.executeCommandGetOutput(
                BASE_COMMAND + " entity -validate -url " + this.hostname + " -type cluster -name " +
                        entityName);
    }*/

    /*@Override
    public String submitAndScheduleViaCLI(String filePath)  {

        return Util.executeCommandGetOutput(
                BASE_COMMAND + " entity -submitAndSchedule -url " + this.hostname +
                        " -type cluster -file " + filePath);
    }

    @Override
    public String scheduleViaCLI(String entityName)  {

        return Util.executeCommandGetOutput(
                BASE_COMMAND + " entity -schedule -url " + this.hostname + " -type cluster -name " +
                        entityName);
    }

    @Override
    public String resumeViaCLI(String entityName)  {

        return Util.executeCommandGetOutput(
                BASE_COMMAND + " entity -resume -url " + this.hostname + " -type cluster -name " +
                        entityName);
    }

    @Override
    public String getStatusViaCLI(String entityName)  {

        return Util.executeCommandGetOutput(
                BASE_COMMAND + " entity -status -url " + this.hostname + " -type cluster -name " +
                        entityName);
    }

    @Override
    public String getEntityDefinitionViaCLI(String entityName)  {

        return Util.executeCommandGetOutput(
                BASE_COMMAND + " entity -definition -url " + this.hostname +
                        " -type cluster -name " + entityName);
    }

    @Override
    public String deleteViaCLI(String entityName)  {

        return Util.executeCommandGetOutput(
                BASE_COMMAND + " entity -delete -url " + this.hostname + " -type cluster -name " +
                        entityName);
    }

    @Override
    public String suspendViaCLI(String entityName)  {

        return Util.executeCommandGetOutput(
                BASE_COMMAND + " entity -suspend -url " + this.hostname + " -type cluster -name " +
                        entityName);
    }

    public String updateViaCLI(String processName, String newProcessFilePath)  {
        return null;
    }
*/
    public String list() throws IOException, InterruptedException {
        return Util.executeCommandGetOutput(
                BASE_COMMAND + " entity -list -url " + this.hostname + " -type cluster");
    }

    @Override
    public String getDependencies(String entityName) throws IOException, InterruptedException {

        return Util.executeCommandGetOutput(
                BASE_COMMAND + " entity -dependency -url " + this.hostname +
                        " -type cluster -name " + entityName);
    }

    /*@Override
    public ProcessInstancesResult getRunningInstance(String processRuningInstance, String name)
     {
        throw new UnsupportedOperationException("Not supported yet.");
    }*/

    @Override
    public List<String> getArchiveInfo() throws IOException, JSchException {

        return Util.getClusterArchiveInfo(this);
    }

    @Override
    public List<String> getStoreInfo() throws IOException, JSchException {

        return Util.getClusterStoreInfo(this);
    }

    @Override
    public ServiceResponse getEntityDefinition(Util.URLS url, String data, String user)
    throws JAXBException, IOException, URISyntaxException, AuthenticationException {
        return getEntityDefinition(this.hostname + url.getValue(), data, user);
    }

    @Override
    public ServiceResponse update(String oldEntity, String newEntity, String user)  {
        throw new UnsupportedOperationException("Not supported yet.");
    }

  @Override
  public ServiceResponse update(String oldEntity,
                                String newEntity,
                                String updateTime, String user) throws IOException, JAXBException {
    return null;
  }

  @Override
    public String toString(Object object) throws JAXBException {
        Cluster processObject = (Cluster) object;

        JAXBContext context = JAXBContext.newInstance(Cluster.class);
        Marshaller um = context.createMarshaller();
        StringWriter writer = new StringWriter();
        um.marshal(processObject, writer);
        return writer.toString();
    }

    /*@Override
    public ProcessInstancesResult getInstanceRerun(String EntityName, String params)
     {
        throw new UnsupportedOperationException("Not supported yet.");
    }*/

    @Override
    public ProcessInstancesResult getProcessInstanceKill(String readEntityName,
                                                         String string, String user)  {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProcessInstancesResult getProcessInstanceRerun(
            String readEntityName, String string, String user)  {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProcessInstancesResult getProcessInstanceResume(
            String readEntityName, String string, String user)  {
        // TODO Auto-generated method stub
        return null;
    }

  @Override
  public InstancesSummaryResult getInstanceSummary(String readEntityName,
                                                   String string
                                                   ) throws
    IOException, URISyntaxException {
    System.out.println("Not Valid for Cluster Entity");
    return null;
  }

    /*@Override
    public String getProcessInstanceStatusViaCli(String EntityName,
                                                 String start, String end, String colos)
     {
        // TODO Auto-generated method stub
        return null;
    }*/

}
