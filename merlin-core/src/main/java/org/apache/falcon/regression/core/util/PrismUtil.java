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
import org.apache.falcon.regression.core.generated.feed.ClusterType;
import org.apache.falcon.regression.core.generated.feed.Feed;
import org.apache.falcon.regression.core.generated.feed.Retention;
import org.apache.falcon.regression.core.generated.feed.Validity;
import org.apache.log4j.Logger;
import org.testng.Assert;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

public class PrismUtil {

    private static Logger logger = Logger.getLogger(PrismUtil.class);

    public static void compareDataStoreStates(List<String> initialState,
                                              List<String> finalState, String filename,
                                              int expectedDiff) {

        if (expectedDiff > -1) {
            finalState.removeAll(initialState);
            Assert.assertEquals(finalState.size(), expectedDiff);
            if (expectedDiff != 0)
                Assert.assertTrue(finalState.get(0).contains(filename));
        } else {
            expectedDiff = expectedDiff * -1;
            initialState.removeAll(finalState);
            Assert.assertEquals(initialState.size(), expectedDiff);
            if (expectedDiff != 0)
                Assert.assertTrue(initialState.get(0).contains(filename));
        }


    }

    public static void compareDataStoreStates(List<String> initialState,
                                              List<String> finalState, int expectedDiff) {

        if (expectedDiff > -1) {
            finalState.removeAll(initialState);
            Assert.assertEquals(finalState.size(), expectedDiff);

        } else {
            expectedDiff = expectedDiff * -1;
            initialState.removeAll(finalState);
            Assert.assertEquals(initialState.size(), expectedDiff);

        }


    }

}
