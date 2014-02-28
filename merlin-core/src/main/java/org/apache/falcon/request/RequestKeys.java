package org.apache.falcon.request;

public class RequestKeys {
    public static final String CONTENT_TYPE_HEADER = "Content-Type";
    public static final String XML_CONTENT_TYPE = "text/xml";

    public static final String AUTH_COOKIE = "hadoop.auth";
    public static final String AUTH_COOKIE_EQ = AUTH_COOKIE + "=";
    public static final String COOKIE = "Cookie";
    public static final String WWW_AUTHENTICATE = "WWW-Authenticate";
    public static final String NEGOTIATE = "Negotiate";
    public static final String CURRENT_USER = System
            .getProperty("user.name");
}
