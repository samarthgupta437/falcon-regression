package org.apache.falcon.regression.core.supportClasses;

public enum ClusterLocationTypes {


  STAGING("staging"),WORKING("working"),TEMP("temp");

  public String getValue() {
    return value;
  }

  private String value;

  private ClusterLocationTypes(String value) {
    this.value = value;
  }
}
