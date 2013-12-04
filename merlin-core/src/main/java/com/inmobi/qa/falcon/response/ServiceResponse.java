/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.inmobi.qa.falcon.response;

/**
 *
 * @author rishu.mehrotra
 */
public class ServiceResponse {
        
        public String message;
        int code;

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public ServiceResponse(String message, int code) {
            this.message = message;
            this.code = code;
        }
        
        public ServiceResponse()
        {
            
        }
        
        
    }
