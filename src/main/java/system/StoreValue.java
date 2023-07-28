package system;

import java.io.Serializable;

public class StoreValue implements Serializable, Comparable<StoreValue> {
  private int version;
  private String value;
  
  public StoreValue(String value, int version) {
    this.version = version;
    this.value = value;
  }

  public StoreValue(String value) {
    this(value, 0);
  }

  public String getValue() {
    return value;
  }

  public int getVersion() {
    return version;
  }

  public void setValue(String value) {
    this.value = value;
    ++version;
  }

  @Override
  public String toString () {
    return value + (version < 0 ? "" : " (v" + version + ")");
  }

  @Override 
  public int compareTo(StoreValue value) {
    if (value == null) {
      return 1;
    }
    return this.version - value.version;
  }
}
