package system;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import akka.actor.ActorRef;

public class PendingRequest {

  public static enum ACT { GET, UPDATE, JOIN };
  
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
    int reqId, key;
    ActorRef client;
    Quorum<T> quorum;
    ACT act;

    public Request (int reqId, ActorRef client, int key, Quorum<T> quorum) {
      this.reqId = reqId;
      this.client = client;
      this.key = key;
      this.quorum = quorum;
    }
  }

  public static class Get<T> extends Request<T> {    
    public Get (int reqId, ActorRef client, int key, Quorum<T> quorum) {
      super(reqId, client, key, quorum);
      this.act = ACT.GET;
    }
  }

  public static class Update<T> extends Request<T> {
    boolean updateLocal;
    String value;
    Set<Integer> involvedNodes;
    
    public Update (int reqId, ActorRef client, Quorum<T> quorum, int key, String value) {
      super(reqId, client, key, quorum);
      this.act = ACT.UPDATE;
      this.value = value;
    }

    public void setInvolvedNodes(Set<Integer> nodes, int idNode) {
      this.involvedNodes = new HashSet<>(nodes);
      this.updateLocal = this.involvedNodes.stream().anyMatch((nId) -> nId == idNode);
    }
  }

  public static class Join<T> extends Request<T> {
    public Join (int reqId, Quorum<T> quorum) {
      super(reqId, null, -1, quorum);
      this.act = ACT.JOIN;
    }
  }
}
