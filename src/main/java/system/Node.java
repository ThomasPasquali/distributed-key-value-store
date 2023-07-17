package system;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.StringJoiner;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class Node extends AbstractActor {

  public static final int T = 1000;
  public static final int N = 3;
  public static final int R = 2;
  public static final int W = 2;
  
  private int reqCount = 0;
  private HashMap<Integer, PendingRequest<StoreValue>> pendingRequests = new HashMap<>();

  protected int idNode;
  protected HashMap<Integer, ActorRef> nodes = new HashMap<>();
  protected HashMap<Integer, StoreValue> store = new HashMap<>() { // FIXME List<StoreValue> keep history??
    @Override
    public String toString () {
      StringJoiner sj = new StringJoiner("\n");
      for (Integer k : store.keySet()) {
        StoreValue v = store.get(k);
        Formatter fmt = new Formatter();
        sj.add(fmt.format("%-3d -> %-15s (v%d)", k, v.getValue(), v.getVersion()).toString());
        fmt.flush();
        fmt.close();
      }
      return sj.toString();
    }
  }; 

  /* -------------------------------------- STATIC MESSAGE TYPES ---------------------------------------- */

  public static enum ACT    { GET, UPDATE };
  public static enum STATUS { OK,  ERROR  };
  
  public static class NodeJoins implements Serializable {
    public final ActorRef newNode;
    public final int id;
    public NodeJoins(ActorRef newNode, int id) {
      this.newNode = newNode;
      this.id = id;
    }
  }

  public static class NodeHello implements Serializable {
    public final int id;
    public NodeHello(int id) {
      this.id = id;
    }
  }

  public static class NodeLeaves implements Serializable {
    public final int id;
    public NodeLeaves(int id) {
      this.id = id;
    }
  }

  public static class Get implements Serializable {
    public final int reqId;
    public final int key;
    public final ACT act;
    public Get(int reqId, int key, ACT act) {
      this.reqId = reqId;
      this.key = key;
      this.act = act;
    }
  }

  public static class Update implements Serializable {
    public final int reqId;
    public final int key;
    public final StoreValue value;
    public Update(int reqId, int key, StoreValue value) {
      this.reqId = reqId;
      this.key = key;
      this.value = value;
    }
  }

  public static class CoordinatorGet implements Serializable { 
    public final int key;
    public CoordinatorGet(int key) {
      this.key = key;
    }
  }

  public static class CoordinatorUpdate implements Serializable { 
    public final int key;
    public final String value;
    public CoordinatorUpdate(int key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  public static class GetResponse implements Serializable {
    public final int reqId;
    public final StoreValue value;
    public final STATUS status;
    public final ACT act;
    public GetResponse(int reqId, StoreValue value, STATUS status, ACT act) {
      this.reqId = reqId;
      this.value = value;
      this.status = status;
      this.act = act;
    }
  }

  public static class Feedback implements Serializable {
    public final String feedback;
    public Feedback(int reqId, StoreValue value, STATUS status, ACT act) {
      String str = "FEEDBACK: " + act + " #" + reqId + " [" + status + "]";
      if (value != null) { str += " | " + value; }
      this.feedback = str;
    }
  }

  /* -------------------------------------- STATIC ---------------------------------------- */

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

  public static class PendingRequest<T> {
    int reqId;
    ActorRef client;
    Quorum<T> quorum;
    
    public PendingRequest (int reqId, ActorRef client, Quorum<T> quorum) {
      this.reqId = reqId;
      this.client = client;
      this.quorum = quorum;
    }
  }

  public static Props props(int id) {
    return Props.create(Node.class, () -> new Node(id));
  }

  /* -------------------------------------- CLASS ---------------------------------------- */

  public Node(int id) {
    this.idNode = id;
  }

  private void log(String log) {
    String timestamp = new SimpleDateFormat("HH:mm:ss.SS").format(new java.util.Date());
    System.out.println(timestamp + ": [Node_" + idNode + "] " + log);
    KeyValStoreSystem.logsMap.get(this.idNode).add(log);
  }

  void multicast(Serializable m) {
    for (ActorRef peer: nodes.values()) {
      peer.tell(m, getSelf());
    }
  }

  /**
   * Messages are sent only to other nodes in the system (from the point of view of the current node)
   * @param m
   * @param nodeIds
   */
  void multicast(Serializable m, int[] nodeIds) {
    for (Integer id: nodeIds) {
      ActorRef peer = nodes.get(id);
      if (peer != null) {
        peer.tell(m, getSelf());
      }
    }
  }

  /**
   * @param key requested item's key
   * 
   * @return a sorted array of ids
   */
  private int[] getRequestResponsibleNodes (int key) { // TODO test me
    int i, count = 0;
    boolean last = true;
    
    List<Integer> ids = new ArrayList<>(nodes.keySet());
    ids.add(idNode);
    Collections.sort(ids);
    for (i = 0; i < ids.size(); ++i) {
      if (ids.get(i) > key) {
        last = false; break;
      }
    }
    
    // TODO 
    if (last) { i--; }
    
    int[] res = new int[N];
    while (count < N) {
      res[count++] = ids.get(i);
      i = (i + 1) % ids.size();
    }
    
    return res;
  }

  void onNodeJoins (NodeJoins nodeJoins) {
    nodes.put(nodeJoins.id, nodeJoins.newNode);
    // Say hello to the new node
    nodeJoins.newNode.tell(new NodeHello(idNode), getSelf());
    log("Node " + nodeJoins.id + " joined! -> N: " + N + ", R: " + R + ", W: " + W);
  }

  void onNodeHello (NodeHello nodeHello) {
    nodes.put(nodeHello.id, getSender());
    log("Hello from " + nodeHello.id + " -> N: " + N + ", R: " + R + ", W: " + W);
  }

  void onNodeLeaves (NodeLeaves nodeLeaves) {
    nodes.remove(nodeLeaves.id);
    log("Node " + nodeLeaves.id + " left! -> N: " + N + ", R: " + R + ", W: " + W);
  }

  void onGet (Get getRequest) {
    log("Get(" + getRequest.key + ") from " + getSender().path().name());
    StoreValue value = store.get(getRequest.key);
    
    ActorRef peer = getSender(), self = getSelf();
    Utils.setTimeout(() -> { // Add artificial delay
      peer.tell(new GetResponse(getRequest.reqId, value == null ? new StoreValue(null, -1) : value, STATUS.OK, getRequest.act), self);
    }, KeyValStoreSystem.delaysMap.get(idNode).get());
  }

  void onUpdate (Update updateRequest) {
    log("Update(" + updateRequest.key + ", " + updateRequest.value + ") from " + getSender().path().name());
    store.put(updateRequest.key, updateRequest.value);
  }  

  void onCoordinatorGet (CoordinatorGet getRequest) throws InterruptedException {
    log("Coordinating: get(" + getRequest.key + ") ");

    int[] respNodes = getRequestResponsibleNodes(getRequest.key);
    int reqId = reqCount++;
    PendingRequest<StoreValue> r = new PendingRequest<>(reqId, getSender(), new Quorum<StoreValue>(R));
    
    if (Arrays.stream(respNodes).anyMatch((nId) -> nId == idNode)) {
      StoreValue value = store.get(getRequest.key);
      r.quorum.inc(value == null ? new StoreValue(null, -1) : value);
    }

    pendingRequests.put(reqId, r);
    multicast(new Get(reqId, getRequest.key, ACT.GET), respNodes);

    ActorRef self = getSelf();
    Utils.setTimeout(() -> {
      PendingRequest<StoreValue> pr = pendingRequests.get(reqId);
      if (pr != null) { // Request still there after timeout
        log("Timeout reached for GetRequest #" + reqId);
        pr.client.tell(new GetResponse(reqId, new StoreValue(null), STATUS.ERROR, ACT.GET), self);
        pendingRequests.remove(reqId);
      }
    }, T);
  }

  void onCoordinatorUpdate (CoordinatorUpdate updateRequest) throws InterruptedException {
    log("Coordinating: update(" + updateRequest.key + ", " + updateRequest.value + ")");

    int reqId = reqCount++;
    int[] respNodes = getRequestResponsibleNodes(updateRequest.key);

    boolean updateLocal = Arrays.stream(respNodes).anyMatch((nId) -> nId == idNode);
    PendingRequest<StoreValue> pending = new PendingRequest<>(reqId, getSender(), new Quorum<StoreValue>(W));
    if (updateLocal) {
      StoreValue value = store.get(updateRequest.key);
      pending.quorum.inc(value == null ? new StoreValue(null, -1) : value);
    }

    pendingRequests.put(reqId, pending);
    multicast(new Get(reqId, updateRequest.key, ACT.UPDATE), respNodes);

    Utils.setTimeout(() -> {
      // Try to get the request after the timeout; if it is still pending return an error
      PendingRequest<StoreValue> req = pendingRequests.get(reqId);
      if (!req.quorum.reached()) { 
        log("Timeout reached for updateRequest #" + reqId);
        req.client.tell(new GetResponse(reqId, new StoreValue(null), STATUS.ERROR, ACT.UPDATE), getSelf());
      } 
      else {        
        StoreValue freshValue = req.quorum.values.get(0);    
        for (StoreValue v : req.quorum.values) {
          if (v.getVersion() > freshValue.getVersion()) {
            freshValue = v;
          }
        }

        StoreValue newValue = new StoreValue(updateRequest.value, freshValue.getVersion() + 1);
        if (updateLocal) { store.put(updateRequest.key, newValue); }

        multicast(new Update(reqId, updateRequest.key, newValue), getRequestResponsibleNodes(updateRequest.key));
      }
      pendingRequests.remove(reqId);
    }, T);

    // KeyValStoreSystem.storesMap.get(this.id).setValue(store.toString());
  }

  void onGetResponse (GetResponse getResponse) {
    PendingRequest<StoreValue> pending = pendingRequests.get(getResponse.reqId);
    String str_log = "Response for Get #" + getResponse.reqId + " from " + getSender().path().name() + ": " + getResponse.value;
    
    if (pending == null || pending.quorum.reached()) {
      log("[IGNORED] " + str_log);
      return;
    }

    log(str_log);
    pending.quorum.inc(new StoreValue(getResponse.value.getValue(), getResponse.value.getVersion()));

    if (pending.quorum.reached()) {
      log("Quorum reached for " + getResponse.act + " #" + getResponse.reqId);
      
      StoreValue freshValue = pending.quorum.values.get(0);
      for (StoreValue v : pending.quorum.values) {
        if (v.getVersion() > freshValue.getVersion()) {
          freshValue = v;
        }
      }

      pending.client.tell(new Feedback(getResponse.reqId, getResponse.act == ACT.GET ? freshValue : null, STATUS.OK, getResponse.act), getSelf());
      if (getResponse.act == ACT.GET) { pendingRequests.remove(getResponse.reqId); }
    }
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(NodeJoins.class, this::onNodeJoins)
      .match(NodeHello.class, this::onNodeHello)
      .match(NodeLeaves.class, this::onNodeLeaves)
      .match(Get.class, this::onGet)
      .match(Update.class, this::onUpdate)      
      .match(CoordinatorGet.class, this::onCoordinatorGet)
      .match(CoordinatorUpdate.class, this::onCoordinatorUpdate)
      .match(GetResponse.class, this::onGetResponse)
      .build();
  }

  /* public Receive crashed() {
    return receiveBuilder()
      .match(Recovery.class, this::onRecovery)
      .matchAny(msg -> {})
      .build();
  } */

  /*/ emulate a crash and a recovery in a given time
  void crash(int recoverIn) {
    getContext().become(crashed());
    print("CRASH!!!");

    // setting a timer to "recover"
    getContext().system().scheduler().scheduleOnce(
        Duration.create(recoverIn, TimeUnit.MILLISECONDS),  
        getSelf(),
        new Recovery(), // message sent to myself
        getContext().system().dispatcher(), getSelf()
        );
  } 
  
  // emulate a delay of d milliseconds
  void delay(int d) {
    try { Thread.sleep(d); } catch (Exception ignored) {}
  }
  */
}
