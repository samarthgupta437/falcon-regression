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

import org.apache.falcon.regression.core.generated.dependencies.Frequency;
import org.apache.falcon.regression.core.generated.feed.ActionType;
import org.apache.falcon.regression.core.generated.feed.Retention;
import org.apache.falcon.regression.core.generated.feed.Validity;
import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.XMLUnit;
import org.testng.log4testng.Logger;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.text.ParseException;

public class XmlUtil {

  static Logger logger = Logger.getLogger(XmlUtil.class);

  public static Validity createValidity(String start, String end) throws ParseException {
        Validity v = new Validity();
        v.setStart(InstanceUtil.oozieDateToDate(start).toDate());
        v.setEnd(InstanceUtil.oozieDateToDate(end).toDate());
        return v;
    }

    public static Retention createRtention(String limit, ActionType action) {
        Retention r = new Retention();
        r.setLimit(new Frequency(limit));
        r.setAction(action);
        return r;
    }


    public static org.apache.falcon.regression.core.generated.process.Validity
    createProcessValidity(
            String startTime, String endTime) throws ParseException {

        org.apache.falcon.regression.core.generated.process.Validity v =
                new org.apache.falcon.regression.core.generated.process.Validity();
        Util.print("instanceUtil.oozieDateToDate(endTime).toDate(): "
                + InstanceUtil.oozieDateToDate(endTime).toDate());
        v.setEnd(InstanceUtil.oozieDateToDate(endTime).toDate());
        v.setStart(InstanceUtil.oozieDateToDate(startTime).toDate());
        return v;

    }

  public static boolean isIdentical(String expected, String actual) throws IOException, SAXException {
    XMLUnit.setIgnoreWhitespace(true);
    XMLUnit.setIgnoreAttributeOrder(true);
    Diff diff = XMLUnit.compareXML(expected, actual);
    logger.info(diff);
    return diff.identical();
  }
}
