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

  public ServiceResponse delete(String url, String data, String user)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {

    return Util.sendRequest(url + "/feed/" + Util.readDatasetName(data) + colo, "delete", user);
  }

  public ServiceResponse getEntityDefinition(String url, String data, String user)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    return Util.sendRequest(url + "/feed/" + Util.readDatasetName(data), "get", user);
  }

  public ServiceResponse getEntityDefinition(Util.URLS url, String data, String user)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    return getEntityDefinition(this.hostname + url.getValue(), data, user);
  }

  public ServiceResponse getStatus(String url, String data, String user)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    return Util.sendRequest(url + "/feed/" + Util.readDatasetName(data) + colo, "get", user);
  }

  public ServiceResponse getStatus(Util.URLS url, String data, String user)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    return getStatus(this.hostname + url.getValue(), data, user);
  }

  public ServiceResponse resume(String url, String data, String user)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    //throw new UnsupportedOperationException("Not supported yet.");
    return Util.sendRequest(url + "/feed/" + Util.readDatasetName(data) + colo, "post", user);
  }

  public ServiceResponse schedule(String url, String data, String user)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    url += "/feed/" + Util.readDatasetName(data) + colo;
    return Util.sendRequest(url, "post", user);
  }

  public ServiceResponse submitAndSchedule(String url, String data, String user)
  throws IOException, URISyntaxException, AuthenticationException {
    System.out.println("Submitting feed: "+data);
    return  Util.sendRequest(url + "/feed" + colo, "post", data, user);
  }

  public ServiceResponse submitAndSchedule(Util.URLS url, String data, String user)
  throws IOException, URISyntaxException, AuthenticationException {
    return submitAndSchedule(this.hostname + url.getValue(), data, user);
  }

  public ServiceResponse submitEntity(String url, String data, String user)
  throws IOException, URISyntaxException, AuthenticationException {

    System.out.println("Submitting feed: "+data);
    url += "/feed" + colo;
    return Util.sendRequest(url, "post", data, user);
  }

  public ServiceResponse suspend(String url, String data, String user)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    return Util.sendRequest(url + "/feed/" + Util.readDatasetName(data) + colo, "post", user);
  }

  public ServiceResponse suspend(Util.URLS url, String data, String user)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    return suspend(this.hostname + url.getValue(), data, user);
  }

  public void validateResponse(String response, APIResult.Status expectedResponse,
                               String filename) throws JAXBException, IOException {
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
  public ServiceResponse submitEntity(Util.URLS url, String data, String user)
  throws IOException, URISyntaxException, AuthenticationException {
    return submitEntity(this.hostname + url.getValue(), data, user);
  }

  @Override
  public ServiceResponse schedule(Util.URLS scheduleUrl, String processData, String user)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    return schedule(this.hostname + scheduleUrl.getValue(), processData, user);
  }

  @Override
  public ServiceResponse delete(Util.URLS deleteUrl, String data, String user)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
     return delete(this.hostname + deleteUrl.getValue(), data, user);
  }

  @Override
  public ServiceResponse resume(Util.URLS url, String data, String user)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {
    // TODO Auto-generated method stub
    return resume(this.hostname + url.getValue(), data, user);
  }

  @Override
  public ProcessInstancesResult getRunningInstance(
          Util.URLS processRunningInstance, String name, String user)
  throws IOException, URISyntaxException, AuthenticationException {

    String url = this.hostname + processRunningInstance.getValue() + "/feed/" + name + allColo;

    return InstanceUtil.sendRequestProcessInstance(url, user);
  }

  @Override
  public ProcessInstancesResult getProcessInstanceStatus(
          String EntityName, String params, String user)
  throws IOException, URISyntaxException, AuthenticationException {

    String url =
      this.hostname + Util.URLS.INSTANCE_STATUS.getValue() + "/" + "feed/" + EntityName +
        "/";

    return InstanceUtil.createAndsendRequestProcessInstance(url, params, allColo, user);
  }


  @Override
  public ProcessInstancesResult getProcessInstanceSuspend(
    String EntityName, String params, String user)
  throws IOException, URISyntaxException, AuthenticationException {
    String url =
      this.hostname + Util.URLS.INSTANCE_SUSPEND.getValue() + "/" + "feed/" + EntityName +
        "/";

    return InstanceUtil.createAndsendRequestProcessInstance(url, params, allColo, user);


  }

  @Override
  public String list() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }


  public ProcessInstancesResult getProcessInstanceResume(String EntityName, String params, String user)
  throws IOException, URISyntaxException, AuthenticationException {
    String url =
      this.hostname + Util.URLS.INSTANCE_RESUME.getValue() + "/" + "feed/" + EntityName +
        "/";
    return InstanceUtil.createAndsendRequestProcessInstance(url, params, allColo, user);

  }

  public ProcessInstancesResult getProcessInstanceKill(String EntityName, String params, String user)
  throws IOException, URISyntaxException, AuthenticationException {
    String url =
      this.hostname + Util.URLS.INSTANCE_KILL.getValue() + "/" + "feed/" + EntityName +
        "/";
    return InstanceUtil.createAndsendRequestProcessInstance(url, params, allColo, user);

  }

  public ProcessInstancesResult getProcessInstanceRerun(String EntityName, String params, String user)
  throws IOException, URISyntaxException, AuthenticationException {
    String url =
      this.hostname + Util.URLS.INSTANCE_RERUN.getValue() + "/" + "feed/" + EntityName +
        "/";
    return InstanceUtil.createAndsendRequestProcessInstance(url, params, allColo, user);

  }

  @Override
  public String getDependencies(String entityName) throws IOException, InterruptedException {

    return Util.executeCommandGetOutput(
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
  public ServiceResponse update(String oldEntity, String newEntity, String user)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {

    String url = this.hostname + Util.URLS.FEED_UPDATE.getValue() + "/" +
      Util.readDatasetName(oldEntity);
      return Util.sendRequest(url + colo, "post", newEntity, user);
  }

  @Override
  public ServiceResponse update(String oldEntity, String newEntity, String updateTime, String user)
  throws IOException, JAXBException, URISyntaxException, AuthenticationException {
    return updateRequestHelper(oldEntity,  newEntity,  updateTime,
      Util.URLS.FEED_UPDATE.getValue(), user) ;
  }

  public ServiceResponse update(String newEntity, String user)
  throws JAXBException, IOException, URISyntaxException, AuthenticationException {

    String url = this.hostname + Util.URLS.FEED_UPDATE.getValue() + "/" +
      Util.readDatasetName(newEntity);
    return Util.sendRequest(url + colo, "post", newEntity, user);
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
}