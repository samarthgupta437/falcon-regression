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
import org.apache.falcon.regression.core.generated.feed.Feed;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.ProcessInstancesResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.testng.Assert;
import org.xml.sax.InputSource;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.List;

public class DataEntityHelperImpl extends IEntityManagerHelper {

  public DataEntityHelperImpl() {
  }

  public DataEntityHelperImpl(String envFileName, String prefix) {
    super(envFileName, prefix);
  }

  public ServiceResponse delete(String url, String data)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {

    return Util.sendRequest(url + "/feed/" + Util.readDatasetName(data) + colo, "delete");
  }

  public ServiceResponse getEntityDefinition(String url, String data)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    return Util.sendRequest(url + "/feed/" + Util.readDatasetName(data), "get");
  }

    /*public ServiceResponse updateFeed(String processName, String newProcess)  {

        String url = this.hostname + Util.URLS.FEED_UPDATE.getValue() + "/" + processName;
        return Util.sendPostRequest(url + colo, newProcess);
    }*/

  public ServiceResponse getEntityDefinition(Util.URLS url, String data)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    //throw new UnsupportedOperationException("Not supported yet.");
    return getEntityDefinition(this.hostname + url.getValue(), data);
  }

  public ServiceResponse getStatus(String url, String data)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    return Util.sendRequest(url + "/feed/" + Util.readDatasetName(data) + colo, "get");
  }

  public ServiceResponse getStatus(Util.URLS url, String data)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    //throw new UnsupportedOperationException("Not supported yet.");
    return getStatus(this.hostname + url.getValue(), data);
  }

  public ServiceResponse resume(String url, String data)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    //throw new UnsupportedOperationException("Not supported yet.");
    return Util.sendRequest(url + "/feed/" + Util.readDatasetName(data) + colo, "post");
  }

  public ServiceResponse schedule(String url, String data)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    url += "/feed/" + Util.readDatasetName(data) + colo;
    return Util.sendRequest(url, "post");
  }

  public ServiceResponse submitAndSchedule(String url, String data)
  throws IOException, URISyntaxException, AuthenticationException {
    //throw new UnsupportedOperationException("Not supported yet.");
    System.out.println("Submitting feed: "+data);
    return Util.sendPostRequest(url + "/feed" + colo, data);
  }

  public ServiceResponse submitAndSchedule(Util.URLS url, String data)
  throws IOException, URISyntaxException, AuthenticationException {
    //throw new UnsupportedOperationException("Not supported yet.");
    return submitAndSchedule(this.hostname + url.getValue(), data);
  }

  public ServiceResponse submitEntity(String url, String data)
  throws IOException, URISyntaxException, AuthenticationException {

    System.out.println("Submitting feed: "+data);
    url += "/feed" + colo;

    return Util.sendPostRequest(url, data);
  }

  public ServiceResponse suspend(String url, String data)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    return Util.sendRequest(url + "/feed/" + Util.readDatasetName(data) + colo, "post");
  }

  public ServiceResponse suspend(Util.URLS url, String data)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    return suspend(this.hostname + url.getValue(), data);
  }

    /*public ServiceResponse validateEntity(String url, String data)  {

        if (!(Thread.currentThread().getStackTrace()[1].getMethodName().contains("Wrong"))) {
            url += "/feed";
        }

        return Util.sendPostRequest(url + colo, data);
    }*/

  public void validateResponse(String response, APIResult.Status expectedResponse,
                               String filename) throws JAXBException, IOException {
    //throw new UnsupportedOperationException("Not supported yet.");
    JAXBContext jc = JAXBContext.newInstance(APIResult.class);

    Unmarshaller u = jc.createUnmarshaller();

    APIResult result = (APIResult) u.unmarshal(new InputSource(new StringReader(response)));

    Assert.assertEquals(expectedResponse, result.getStatus(),
      "Status message does not match with expected one!");

    if (expectedResponse.equals(APIResult.Status.FAILED)) {
      //now to check for the correct error message!
      Assert.assertEquals(result.getMessage(), Util.getExpectedErrorMessage(filename),
        "Error message does not match in failure case!");
    } else {
      Assert.assertEquals(result.getMessage(), "Validate successful",
        "validation success message does not match in valid case!");
    }
  }

  @Override
  public ServiceResponse submitEntity(Util.URLS url, String data)
  throws IOException, URISyntaxException, AuthenticationException {
    return submitEntity(this.hostname + url.getValue(), data);
  }

  @Override
  public ServiceResponse schedule(Util.URLS scheduleUrl, String processData)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    return schedule(this.hostname + scheduleUrl.getValue(), processData);
  }

  @Override
  public ServiceResponse delete(Util.URLS deleteUrl, String data)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {

    //String url=deleteUrl.getValue()+"/feed/"+Util.readDatasetName(data);
    //return Util.sendPostRequest(url);

    return delete(this.hostname + deleteUrl.getValue(), data);

  }

  @Override
  public ServiceResponse resume(Util.URLS url, String data)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    // TODO Auto-generated method stub
    return resume(this.hostname + url.getValue(), data);
  }


    /*@Override
    public ProcessInstancesResult getRunningInstance(
            String processRuningInstance, String name)  {

        String url = this.hostname + processRuningInstance + "/" + name + allColo;

        return InstanceUtil.sendRequestProcessInstance(url);
    }*/

  @Override
  public ProcessInstancesResult getRunningInstance(
    Util.URLS processRuningInstance, String name)
  throws IOException, URISyntaxException, AuthenticationException {

    String url = this.hostname + processRuningInstance.getValue() + "/feed/" + name + allColo;

    return InstanceUtil.sendRequestProcessInstance(url);
  }

  @Override
  public ProcessInstancesResult getProcessInstanceStatus(
    String EntityName, String params)
  throws IOException, URISyntaxException, AuthenticationException {

    String url =
      this.hostname + Util.URLS.INSTANCE_STATUS.getValue() + "/" + "feed/" + EntityName +
        "/";

    return InstanceUtil.createAndsendRequestProcessInstance(url, params, allColo);
  }


  @Override
  public ProcessInstancesResult getProcessInstanceSuspend(
    String EntityName, String params)
  throws IOException, URISyntaxException, AuthenticationException {
    String url =
      this.hostname + Util.URLS.INSTANCE_SUSPEND.getValue() + "/" + "feed/" + EntityName +
        "/";

    return InstanceUtil.createAndsendRequestProcessInstance(url, params, allColo);


  }

  @Override
  public String list() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }


  public ProcessInstancesResult getProcessInstanceResume(String EntityName, String params)
  throws IOException, URISyntaxException, AuthenticationException {
    String url =
      this.hostname + Util.URLS.INSTANCE_RESUME.getValue() + "/" + "feed/" + EntityName +
        "/";
    return InstanceUtil.createAndsendRequestProcessInstance(url, params, allColo);

  }

  public ProcessInstancesResult getProcessInstanceKill(String EntityName, String params)
  throws IOException, URISyntaxException, AuthenticationException {
    String url =
      this.hostname + Util.URLS.INSTANCE_KILL.getValue() + "/" + "feed/" + EntityName +
        "/";
    return InstanceUtil.createAndsendRequestProcessInstance(url, params, allColo);

  }

  public ProcessInstancesResult getProcessInstanceRerun(String EntityName, String params)
  throws IOException, URISyntaxException, AuthenticationException {
    String url =
      this.hostname + Util.URLS.INSTANCE_RERUN.getValue() + "/" + "feed/" + EntityName +
        "/";
    return InstanceUtil.createAndsendRequestProcessInstance(url, params, allColo);

  }


    /*public String writeEntityToFile(String entity)  {
        File file = new File("/tmp/" + Util.readDatasetName(entity) + ".xml");
        BufferedWriter bf = new BufferedWriter(new FileWriter(file));
        bf.write(entity);
        bf.close();
        return "/tmp/" + Util.readDatasetName(entity) + ".xml";
    }*/

    /*@Override
    public String submitEntityViaCLI(String filePath)  {

        return Util.executeCommand(
                BASE_COMMAND + " entity -submit -url " + this.hostname + " -type feed -file " +
                        filePath);
    }

    @Override
    public String validateEntityViaCLI(String entityName)  {

        return Util.executeCommand(
                BASE_COMMAND + " entity -validate -url " + this.hostname + " -type feed -name " +
                        entityName);
    }

    @Override
    public String submitAndScheduleViaCLI(String filePath)  {

        return Util.executeCommand(
                BASE_COMMAND + " entity -submitAndSchedule -url " + this.hostname +
                        " -type feed -file " + filePath);
    }

    @Override
    public String scheduleViaCLI(String entityName)  {

        return Util.executeCommand(
                BASE_COMMAND + " entity -schedule -url " + this.hostname + " -type feed -name " +
                        entityName);
    }

    @Override
    public String resumeViaCLI(String entityName)  {

        return Util.executeCommand(
                BASE_COMMAND + " entity -resume -url " + this.hostname + " -type feed -name " +
                        entityName);
    }

    @Override
    public String getStatusViaCLI(String entityName)  {

        return Util.executeCommand(
                BASE_COMMAND + " entity -status -url " + this.hostname + " -type feed -name " +
                        entityName);
    }

    @Override
    public String getEntityDefinitionViaCLI(String entityName)  {

        return Util.executeCommand(
                BASE_COMMAND + " entity -definition -url " + this.hostname + " -type feed -name " +
                        entityName);
    }

    @Override
    public String deleteViaCLI(String entityName)  {

        return Util.executeCommand(
                BASE_COMMAND + " entity -delete -url " + this.hostname + " -type feed -name " +
                        entityName);
    }

    @Override
    public String suspendViaCLI(String entityName)  {

        return Util.executeCommand(
                BASE_COMMAND + " entity -suspend -url " + this.hostname + " -type feed -name " +
                        entityName);
    }

    public String updateViaCLI(String processName, String newProcessFilePath)  {
        return null;
    }

    public String list()  {
        return Util.executeCommand(
                BASE_COMMAND + " entity -list -url " + this.hostname + " -type feed");
    }*/

  @Override
  public String getDependencies(String entityName) throws IOException, InterruptedException {

    return Util.executeCommand(
      BASE_COMMAND + " entity -dependency -url " + this.hostname + " -type feed -name " +
        entityName);
  }


  @Override
  public List<String> getArchiveInfo() throws IOException, JSchException {

    return Util.getDataSetArchiveInfo(this);
  }

  @Override
  public List<String> getStoreInfo() throws IOException, JSchException {

    return Util.getDataSetStoreInfo(this);
  }

  @Override
  public ServiceResponse update(String oldEntity, String newEntity)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {

    String url = this.hostname + Util.URLS.FEED_UPDATE.getValue() + "/" +
      Util.readDatasetName(oldEntity);
    return Util.sendPostRequest(url + colo, newEntity);
  }

  @Override
  public ServiceResponse update(String oldEntity, String newEntity, String updateTime)
  throws IOException, JAXBException, URISyntaxException, AuthenticationException {
    return updateRequestHelper(oldEntity,  newEntity,  updateTime,
      Util.URLS.FEED_UPDATE.getValue()) ;
  }

  public ServiceResponse update(String newEntity)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {

    String url = this.hostname + Util.URLS.FEED_UPDATE.getValue() + "/" +
      Util.readDatasetName(newEntity);
    return Util.sendPostRequest(url + colo, newEntity);
  }

  @Override
  public String toString(Object object) throws JAXBException {
    Feed processObject = (Feed) object;

    JAXBContext context = JAXBContext.newInstance(Feed.class);
    Marshaller um = context.createMarshaller();
    StringWriter writer = new StringWriter();
    um.marshal(processObject, writer);
    return writer.toString();
  }

/*
    @Override
    public ProcessInstancesResult getInstanceRerun(String EntityName, String params)
     {
        throw new UnsupportedOperationException("Not supported yet.");
    }
*/

    /*@Override
    public String getProcessInstanceStatusViaCli(String EntityName,
                                                 String start, String end, String colos)
     {
        // TODO Auto-generated method stub
        return null;
    }*/
}
