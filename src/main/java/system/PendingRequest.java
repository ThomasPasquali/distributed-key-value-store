package system;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import akka.actor.ActorRef;

public class PendingRequest {

  public static enum ACT { GET, UPDATE };
  
  public static class Quorum<T> {
    private int quorumThreshold;
    private int quorumValue;
    List<T> values = new ArrayList<>(10);
    
    public Quorum (int quorumThreshold) {
      this.quorumThreshold = quorumThreshold;
    }
    
    public int inc (T value) {
      values.add(value);
      return ++quorumValue;
    }
    
    public boolean reached () {
      return quorumValue >= quorumThreshold;
    }
  }

  public static abstract class Request<T> {
    int reqId;
    ActorRef client;
    Quorum<T> quorum;
    ACT act;

    public Request (int reqId, ActorRef client, Quorum<T> quorum) {
      this.reqId = reqId;
      this.client = client;
      this.quorum = quorum;
    }
  }

  public static class Get<T> extends Request<T> {    
    public Get (int reqId, ActorRef client, Quorum<T> quorum) {
      super(reqId, client, quorum);
      this.act = ACT.GET;
    }
  }

  public static class Update<T> extends Request<T> {
    boolean updateLocal;
    int key; 
    String value;
    Set<Integer> involvedNodes;
    
    public Update (int reqId, ActorRef client, Quorum<T> quorum, int key, String value) {
      super(reqId, client, quorum);
      this.act = ACT.UPDATE;
      this.key = key;
      this.value = value;
    }

    public void setInvolvedNodes(Set<Integer> nodes, int idNode) {
      this.involvedNodes = new HashSet<>(nodes);
      this.updateLocal = this.involvedNodes.stream().anyMatch((nId) -> nId == idNode);
    }
  }
}
