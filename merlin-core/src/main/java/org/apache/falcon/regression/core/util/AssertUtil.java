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

import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.util.List;

public class AssertUtil {

    public static void failIfStringFoundInPath(
            List<Path> paths, String... shouldNotBePresent) {

        for (Path path : paths) {
            for (String aShouldNotBePresent : shouldNotBePresent)
                if (path.toUri().toString().contains(aShouldNotBePresent))
                    Assert.assertTrue(false,
                            "String " + aShouldNotBePresent + " was not expected in path " +
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
                APIResult.Status.SUCCEEDED);
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 200);
        Assert.assertNotNull(Util.parseResponse(response).getMessage());

    }

    public static void assertFailed(ServiceResponse response, String message) throws JAXBException {
        if (response.message.equals("null"))
            Assert.assertTrue(false, "response message should not be null");

        Assert.assertEquals(Util.parseResponse(response).getStatus(),
                APIResult.Status.FAILED, message);
        Assert.assertEquals(Util.parseResponse(response).getStatusCode(), 400,
                message);
        Assert.assertNotNull(Util.parseResponse(response).getRequestId());
    }

}
