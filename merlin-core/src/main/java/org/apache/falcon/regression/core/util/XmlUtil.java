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

public class XmlUtil {

    public static Validity createValidity(String start, String end) throws Exception {
        Validity v = new Validity();
        v.setStart(InstanceUtil.oozieDateToDate(start).toDate());
        v.setEnd(InstanceUtil.oozieDateToDate(end).toDate());
        //v.setTimezone(timeZone);
        return v;
    }

    public static Retention createRtention(String limit, ActionType action) {
        Retention r = new Retention();
        r.setLimit(new Frequency(limit));
        r.setAction(action);
        return r;
    }

    /*	public static String marshalUnmarshalCLuster(String originalXml) throws JAXBException
        {
            JAXBContext jc=JAXBContext.newInstance(Cluster.class);
            Unmarshaller u=jc.createUnmarshaller();
            Cluster c = (Cluster)u.unmarshal((new StringReader(originalXml)));



            java.io.StringWriter sw = new StringWriter();
            Marshaller marshaller = jc.createMarshaller();
            marshaller.marshal(c,sw);

            c = (Cluster)u.unmarshal((new StringReader(sw.toString())));


            com.thoughtworks.xstream.XStream xstream = new com.thoughtworks.xstream.XStream(new
            com.thoughtworks
            .xstream.converters.reflection.Sun14ReflectionProvider(
                       new com.thoughtworks.xstream.converters.reflection.FieldDictionary(new com
                       .thoughtworks
                       .xstream.converters.reflection.ImmutableFieldKeySorter())),
                       new com.thoughtworks.xstream.io.xml.DomDriver("utf-8"));



                    String thisStr = xstream.toXML(c);


            return thisStr;




        }
    */
    public static org.apache.falcon.regression.core.generated.process.Validity
    createProcessValidity(
            String startTime, String endTime) throws Exception {

        org.apache.falcon.regression.core.generated.process.Validity v =
                new org.apache.falcon.regression.core.generated.process.Validity();
        Util.print("instanceUtil.oozieDateToDate(endTime).toDate(): "
                + InstanceUtil.oozieDateToDate(endTime).toDate());
        v.setEnd(InstanceUtil.oozieDateToDate(endTime).toDate());
        v.setStart(InstanceUtil.oozieDateToDate(startTime).toDate());
        return v;

    }
}
