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

public class GraphUtil {
    public enum URLS {
        SERIALIZE("/api/graphs/lineage/serialize"),
        VERTICES("/api/graphs/lineage/vertices"),
        VERTICES_ALL("/api/graphs/lineage/vertices/all"),
        VERTICES_PROPERTIES("/api/graphs/lineage/vertices/properties/"),
        EDGES("/api/graphs/lineage/edges"),
        EDGES_ALL("/api/graphs/lineage/edges/all");

        private final String url;

        URLS(String url) {
            this.url = url;
        }

        public String getValue() {
            return this.url;
        }
    }

}
