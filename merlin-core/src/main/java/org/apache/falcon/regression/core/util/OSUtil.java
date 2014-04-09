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

import org.apache.commons.lang.StringUtils;

public class OSUtil {

    public static boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().startsWith
            ("windows") ? true : false;

    public static String SEPARATOR = System.getProperty("file.separator", "/");
    public static String RESOURCES = String.format("src%stest%sresources%s", SEPARATOR, SEPARATOR, SEPARATOR);
    public static String RESOURCES_OOZIE = String.format(RESOURCES + "oozie%s", SEPARATOR);
    public static String OOZIE_EXAMPLE_INPUT_DATA =
            String.format(RESOURCES + "OozieExampleInputData%s", SEPARATOR);
    public static String NORMAL_INPUT =
            String.format(OOZIE_EXAMPLE_INPUT_DATA + "normalInput%s", SEPARATOR);

    public static String getPath(String... pathParts) {
        return StringUtils.join(pathParts, OSUtil.SEPARATOR);
    }
}
