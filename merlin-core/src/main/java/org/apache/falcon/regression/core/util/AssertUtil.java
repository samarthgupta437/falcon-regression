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

package org.apache.falcon.regression.core.util;

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.enumsAndConstants.ENTITY_TYPE;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.util.List;

public class AssertUtil {

    public static void failIfStringFoundInPath(
            List<Path> paths, String... shouldNotBePresent) {

        for (Path path : paths) {
            for (String aShouldNotBePresent : shouldNotBePresent)
                if (path.toUri().toString().contains(aShouldNotBePresent))
                    Assert.fail("String " + aShouldNotBePresent + " was not expected in path " +
                                    path.toUri().toString());
        }
    }

    public static void checkForPathsSizes(List<Path> expected,
                                          List<Path> actual) {

        Assert.assertEquals(actual.size(), expected.size(),
                "array size of the 2 paths array list is not the same");
    }

    public static void assertSucceeded(ServiceResponse response) throws JAXBException {
        Assert.assertEquals(Util.parseResponse(response).getStatus(),
                APIResult.Status.SUCCEEDED, "Status should be SUCCEEDED");
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200, "Status code should be 200");
        Assert.assertNotNull(Util.parseResponse(response).getMessage(), "Status message is null");

    }

  public static void assertSucceeded(APIResult response) throws JAXBException {
    Assert.assertEquals(response.getStatus(), APIResult.Status.SUCCEEDED, "Status should be SUCCEEDED");
    Assert.assertEquals(response.getStatusCode(), 200, "Status code should be 200");
    Assert.assertNotNull(response.getMessage(), "Status message is null");

  }


    public static void assertFailed(final ServiceResponse response, final String message) throws JAXBException {
        assertFailedWithStatus(response, 400, message);
    }

    public static void assertFailedWithStatus(final ServiceResponse response, final int statusCode, final String message) throws JAXBException {
        if (response.message.equals("null"))
            Assert.fail("response message should not be null");

        Assert.assertEquals(Util.parseResponse(response).getStatus(),
                APIResult.Status.FAILED, message);
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), statusCode,
                message);
        Assert.assertNotNull(Util.parseResponse(response).getRequestId(), "RequestId is null");
    }

  public static void assertFailed(APIResult response, String message) throws JAXBException {
    if (response.getMessage().equals("null"))
      Assert.assertTrue(false, "response message should not be null");

    Assert.assertEquals(response.getStatus(),
      APIResult.Status.FAILED, message);
    Assert.assertEquals(response.getStatusCode(), 400,
      message);
    Assert.assertNotNull(response.getRequestId());
  }

  public static void assertPartial(ServiceResponse response) throws JAXBException {
    Assert.assertEquals(Util.parseResponse(response).getStatus(),APIResult.Status.PARTIAL, "Status should be PARTIAL");
    Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 400, "Status code should be 400");
    Assert.assertNotNull(Util.parseResponse(response).getMessage(), "Status message is null");

  }
    public static void checkStatus(OozieClient oozieClient, ENTITY_TYPE entityType, String data, Job.Status expectedStatus) throws Exception {
        String name = null;
        if(entityType == ENTITY_TYPE.FEED) {
            name = Util.readDatasetName(data);
        } else if(entityType == ENTITY_TYPE.PROCESS) {
            name = Util.readEntityName(data);
        }
        Assert.assertEquals(Util.verifyOozieJobStatus(oozieClient, name, entityType, expectedStatus), true,
                "Status should be " + expectedStatus);
    }

    public static void checkStatus(OozieClient oozieClient, ENTITY_TYPE entityType, Bundle bundle, Job.Status expectedStatus) throws Exception {
        String data = null;
        if(entityType == ENTITY_TYPE.FEED) {
            data = bundle.getDataSets().get(0);
        } else if(entityType == ENTITY_TYPE.PROCESS) {
            data = bundle.getProcessData();
        }
        checkStatus(oozieClient, entityType, data, expectedStatus);
    }

    public static void checkNotStatus(OozieClient oozieClient, ENTITY_TYPE entityType, String data, Job.Status expectedStatus) throws Exception {
        String processName = null;
        if(entityType == ENTITY_TYPE.FEED) {
            processName = Util.readDatasetName(data);
        } else if(entityType == ENTITY_TYPE.PROCESS) {
            processName = Util.readEntityName(data);
        }
        Assert.assertNotEquals(Util.getOozieJobStatus(oozieClient, processName,
                entityType), expectedStatus, "Status should not be " + expectedStatus);
    }

    public static void checkNotStatus(OozieClient oozieClient, ENTITY_TYPE entityType, Bundle bundle, Job.Status expectedStatus) throws Exception {
        String data = null;
        if(entityType == ENTITY_TYPE.FEED) {
            data = bundle.getDataSets().get(0);
        } else if(entityType == ENTITY_TYPE.PROCESS) {
            data = bundle.getProcessData();
        }
        checkNotStatus(oozieClient, entityType, data, expectedStatus);
    }

}
