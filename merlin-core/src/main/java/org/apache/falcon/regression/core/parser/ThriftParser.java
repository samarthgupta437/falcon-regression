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

package org.apache.falcon.regression.core.parser;
//package com.inmobi.qa.ivory.parser;
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileReader;
//import java.util.ArrayList;
//
//import org.apache.commons.codec.binary.Base64;
//import org.apache.thrift.TBase;
//import org.apache.thrift.TDeserializer;
//
//import com.inmobi.qa.ivory.util.Util;
//import com.inmobi.types.adserving.AdRR;
//
//public class ThriftParser<T extends TBase> {
//
//
//	public T parseThrift(File thriftFile, T returnObject) throws Exception 
//	{
//
//		FileReader r = new FileReader(thriftFile);
//		BufferedReader reader = new BufferedReader (r);
//		String line = reader.readLine();
//		while(line!=null)
//		{
//			Base64 base64 = new Base64();
//			TDeserializer ud= new TDeserializer();
//			ud.deserialize((TBase) returnObject,base64.decode(line.getBytes()));
//			System.out.println(returnObject);
//
//			line = reader.readLine();
//
//
//		}
//
//		return returnObject;
//
//	}
//
//
//	public T parseThrift(File thriftFile, T returnObject,int objectNumber) throws Exception 
//	{
//
//		FileReader r = new FileReader(thriftFile);
//		BufferedReader reader = new BufferedReader (r);
//		String line = reader.readLine();
//		int i =0;
//		while(line!=null)
//		{
//			i++;
//
//			if(i == objectNumber){
//				Base64 base64 = new Base64();
//				TDeserializer ud= new TDeserializer();
//				ud.deserialize((TBase) returnObject,base64.decode(line.getBytes()));
//				//	System.out.println(returnObject);
//				break;
//			}
//			line = reader.readLine();
//
//		}
//
//		return returnObject;
//
//	}
//
//	
//
//	
//
//	public AdRR parseThrift(File thriftFile,int numberOfImpressions) throws Exception 
//	{
//
//		AdRR returnObject = new AdRR();
//		FileReader r = new FileReader(thriftFile);
//		BufferedReader reader = new BufferedReader (r);
//		String line = reader.readLine();
//		
//		while(line!=null)
//		{
//
//			Base64 base64 = new Base64();
//			TDeserializer ud= new TDeserializer();
//			ud.deserialize((TBase) returnObject,base64.decode(line.getBytes()));
//			if( (returnObject.getImpressions() != null) && (returnObject.getImpressions().size() ==
// numberOfImpressions)){
//				break;
//			}
//			line = reader.readLine();
//
//		}
//
//		return returnObject;
//
//	}
//
//
//	public T parseThrift(String fileLocation, T returnObject) throws Exception
//	{
//		File thriftFile = new File(fileLocation);
//		FileReader r = new FileReader(thriftFile);
//		BufferedReader reader = new BufferedReader (r);
//		StringBuilder data= new StringBuilder();
//		String line = reader.readLine();
//		while(line!=null)
//		{
//			data.append(line);
//			line = reader.readLine();
//		}
//		Base64 base64 = new Base64();
//		TDeserializer ud= new TDeserializer();
//
//		ud.deserialize((TBase) returnObject,base64.decode(data.toString().getBytes())); 
//		return returnObject;
//
//	}
//
//	public ArrayList<T> parseThrift(File thriftFile, ArrayList<T> returnObject) throws Exception 
//	{
//
//		FileReader r = new FileReader(thriftFile);
//		BufferedReader reader = new BufferedReader (r);
//		StringBuilder data= new StringBuilder();
//		String line = reader.readLine();
//		while(line!=null)
//		{
//			data.append(line);
//			line = reader.readLine();
//		}
//		Base64 base64 = new Base64();
//		TDeserializer ud= new TDeserializer();
//
//		ud.deserialize((TBase) returnObject,base64.decode(data.toString().getBytes())); 
//		return returnObject;
//
//	}
//
//
//	public AdRR parseThrift(File thriftFile, int numberOfImpressions, int afterNumberOFRequest) 
//	throws Exception{
//		
//		FileReader r = new FileReader(thriftFile);
//		BufferedReader reader = new BufferedReader (r);
//		String line = reader.readLine();
//		int foundTillNow = 0;
//		int count = 0 ;
//		while(line!=null)
//		{
//
//			Base64 base64 = new Base64();
//			TDeserializer ud= new TDeserializer();
//			
//			AdRR returnObject = new AdRR();
//			ud.deserialize((TBase) returnObject,base64.decode(line.getBytes()));
//		
//		//	Log.info("count: "+count+" returnObject.getImpressions().size(): "+returnObject
// .getImpressions().size());
//			
//			if( (returnObject.getImpressions() != null) && (returnObject.getImpressions().size() ==
// numberOfImpressions)){
//				if(foundTillNow == afterNumberOFRequest){
//					//Util.print(String.valueOf(returnObject.getRequest().getN_ads_served()));
//					//Util.print(returnObject.toString());
//					return returnObject;
//				}
//					
//				else 
//					foundTillNow++;
//					
//				
//			}
//			else if(returnObject.getImpressions() == null && numberOfImpressions==0){
//				if(foundTillNow == afterNumberOFRequest)
//				{
//				//	Util.print(String.valueOf(returnObject.getRequest().getN_ads_served()));
//				//	Util.print(returnObject.toString());
//					return returnObject;
//				}
//				else 
//					foundTillNow++;
//			}
//				
//			line = reader.readLine();
//
//		}
//
//		Util.print("unable to find requied AdRR");
//		return null;
//		
//	}
//
//}
