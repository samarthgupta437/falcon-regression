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

//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference
// Implementation,
// vJAXB 2.1.10 in JDK 6
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2013.05.29 at 03:46:24 PM GMT+05:30 
//


package org.apache.falcon.regression.core.generated.process;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for input complex type.
 * <p/>
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p/>
 * <pre>
 * &lt;complexType name="input">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;attribute name="name" use="required" type="{uri:falcon:process:0.1}IDENTIFIER" />
 *       &lt;attribute name="feed" use="required" type="{uri:falcon:process:0.1}IDENTIFIER" />
 *       &lt;attribute name="start" use="required" type="{http://www.w3
 *       .org/2001/XMLSchema}string" />
 *       &lt;attribute name="end" use="required" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="partition" type="{http://www.w3.org/2001/XMLSchema}string" />
 *       &lt;attribute name="optional" type="{http://www.w3.org/2001/XMLSchema}boolean"
 *       default="false" />
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "input")
public class Input {

    @XmlAttribute(required = true)
    protected String name;
    @XmlAttribute(required = true)
    protected String feed;
    @XmlAttribute(required = true)
    protected String start;
    @XmlAttribute(required = true)
    protected String end;
    @XmlAttribute
    protected String partition;
    @XmlAttribute
    protected Boolean optional;

    /**
     * Gets the value of the name property.
     *
     * @return possible object is
     * {@link String }
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the value of the name property.
     *
     * @param value allowed object is
     *              {@link String }
     */
    public void setName(String value) {
        this.name = value;
    }

    /**
     * Gets the value of the feed property.
     *
     * @return possible object is
     * {@link String }
     */
    public String getFeed() {
        return feed;
    }

    /**
     * Sets the value of the feed property.
     *
     * @param value allowed object is
     *              {@link String }
     */
    public void setFeed(String value) {
        this.feed = value;
    }

    /**
     * Gets the value of the start property.
     *
     * @return possible object is
     * {@link String }
     */
    public String getStart() {
        return start;
    }

    /**
     * Sets the value of the start property.
     *
     * @param value allowed object is
     *              {@link String }
     */
    public void setStart(String value) {
        this.start = value;
    }

    /**
     * Gets the value of the end property.
     *
     * @return possible object is
     * {@link String }
     */
    public String getEnd() {
        return end;
    }

    /**
     * Sets the value of the end property.
     *
     * @param value allowed object is
     *              {@link String }
     */
    public void setEnd(String value) {
        this.end = value;
    }

    /**
     * Gets the value of the partition property.
     *
     * @return possible object is
     * {@link String }
     */
    public String getPartition() {
        return partition;
    }

    /**
     * Sets the value of the partition property.
     *
     * @param value allowed object is
     *              {@link String }
     */
    public void setPartition(String value) {
        this.partition = value;
    }

    /**
     * Gets the value of the optional property.
     *
     * @return possible object is
     * {@link Boolean }
     */
    public boolean isOptional() {
        if (optional == null) {
            return false;
        } else {
            return optional;
        }
    }

    /**
     * Sets the value of the optional property.
     *
     * @param value allowed object is
     *              {@link Boolean }
     */
    public void setOptional(Boolean value) {
        this.optional = value;
    }

}