package org.apache.falcon.regression.core.supportClasses;

public enum OozieActions {

      RECORD_SIZE("recordsize"),SHOULD_RECORD("should-record"),
  USER_WORKFLOW("user-workflow"),USER_OOZIE_WORKFLOW("user-oozie-workflow"),
  SUCCEEDED_POST_PROCESSING("succeeded-post-processing"),END("end");

  public String getValue() {
    return value;
  }

  private String value;

  private OozieActions(String value) {
    this.value = value;
  }
}
