package edu.buffalo.cse.cse486586.simpledynamo;

public class Replica {

    public void setOriginPort(String originPort) {
        this.originPort = originPort;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    String originPort;
    String key;
    String value;

    public String getOriginPort() {
        return originPort;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
