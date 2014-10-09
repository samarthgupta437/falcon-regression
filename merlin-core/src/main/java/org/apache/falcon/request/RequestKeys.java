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

package org.apache.falcon.request;
import org.apache.falcon.regression.core.util.Config;
import org.apache.commons.lang.StringUtils;

public class RequestKeys {
    public static final String CONTENT_TYPE_HEADER = "Content-Type";
    public static final String XML_CONTENT_TYPE = "text/xml";

    public static final String AUTH_COOKIE = "hadoop.auth";
    public static final String AUTH_COOKIE_EQ = AUTH_COOKIE + "=";
    public static final String COOKIE = "Cookie";
    public static final String WWW_AUTHENTICATE = "WWW-Authenticate";
    public static final String NEGOTIATE = "Negotiate";
    public static String CURRENT_USER = getCurrentUSer();

    private static String getCurrentUSer() {
        String configUser = Config.getProperty("REQUEST.USER");
        if (StringUtils.isNotBlank(configUser)) {
            return configUser;
        }
        else {
              return  System.getProperty("user.name");
        }
    }
    public static void setCURRENT_USER(String CURRENT_USER) {
        RequestKeys.CURRENT_USER = CURRENT_USER;
    }

}
