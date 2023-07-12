package system;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.StringJoiner;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class Node extends AbstractActor {

  /* STATIC */

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
    public Get(int reqId, int key) {
      this.reqId = reqId;
      this.key = key;
    }
  }
  public static class CoordinatorGet implements Serializable { // FIXME do we need this or can just use Get?
    public final int key;
    public CoordinatorGet(int key) {
      this.key = key;
    }
  }
  public static class GetResponse implements Serializable {
    public static enum STATUS { OK, ERROR };
    public final int reqId;
    public final String value;
    public final STATUS status;
    public GetResponse(int reqId, String value, STATUS status) {
      this.reqId = reqId;
      this.value = value;
      this.status = status;
    }
  }

  public static class Update implements Serializable {
    public final int reqId;
    public final int key;
    public final String value;
    public Update(int reqId, int key, String value) {
      this.reqId = reqId;
      this.key = key;
      this.value = value;
    }
  }
  public static class CoordinatorUpdate implements Serializable { // FIXME do we need this or can just use Update?
    public final int key;
    public final String value;
    public CoordinatorUpdate(int key, String value) {
      this.key = key;
      this.value = value;
    }
  }
  public static class UpdateResponse implements Serializable {
    public static enum STATUS { OK, ERROR };
    public final int reqId;
    public final STATUS status;
    public UpdateResponse(int reqId, STATUS status) {
      this.reqId = reqId;
      this.status = status;
    }
  }

  // Just for the UI
  public static class Feedback implements Serializable {
    public final String feedback;
    public Feedback(String feedback) {
      this.feedback = feedback;
    }
  }

  public static class StoreValue implements Serializable {
    private int version;
    private String value;
    public StoreValue(String value) {
      this.version = 0;
      this.value = value;
    }
    public int getVersion() {
      return version;
    }
    public void setValue(String value) {
      this.value = value;
      ++version;
    }
    public String getValue() {
      return value;
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

  static public Props props(int id) {
    return Props.create(Node.class, () -> new Node(id));
  }

  /* CLASS */

  public static final int T = 5000;
  // FIXME should they be static?
  public int N = 0;
  public int R = 0;
  public int W = 0;
  private int reqCount = 0;
  private HashMap<Integer, PendingRequest<String>> pendingRequests = new HashMap<>();

  protected int id;
  protected HashMap<Integer, ActorRef> nodes = new HashMap<>();
  protected HashMap<Integer, StoreValue> store = new HashMap<>() { // FIXME List<StoreValue> keep history??
    @Override
    public String toString () {
      StringJoiner sj = new StringJoiner("\n");
      for (Integer k : store.keySet()) {
        StoreValue v = store.get(k);
        Formatter fmt = new Formatter();
        sj.add(fmt.format("%-3d -> %-15s (v%d)", k, v.value, v.version).toString());
        fmt.flush();
        fmt.close();
      }
      return sj.toString();
    }
  }; 

  public Node(int id) {
    this.id = id;
  }

  private void log(String log) {
    System.out.println("[Node_" + id + "] " + log);
    KeyValStoreSystem.logsMap.get(this.id).add(log);
  }

  void multicast(Serializable m) {
    for (ActorRef peer: nodes.values()) {
      peer.tell(m, getSelf());
    }
  }

  void multicast(Serializable m, int[] nodeIds) {
    for (Integer id: nodeIds) {
      ActorRef peer = nodes.get(id);
      if (peer != null) {
        peer.tell(m, getSelf());
      }
    }
  }

  private void computeQuorumConstants () { // FIXME compute wisely
    N = Math.min(4, nodes.size()); 
    R = N / 2 + 1;
    W = N / 2 + 1;
  }

  private int[] getRequestResponsibleNodes (int key, int n) { // TODO test me
    int i, count = 0;
    List<Integer> ids = new ArrayList<>(nodes.keySet());
    Collections.sort(ids);
    for (i = 0; i < ids.size(); ++i) { // TODO include current node
      if (ids.get(i) > key) {
        break;
      }
    }
    int[] res = new int[n];
    while (count < n) {
      res[count++] = ids.get(i);
      i = (i + 1) % ids.size();
    }
    return res;
  }

  void onNodeJoins (NodeJoins nodeJoins) {
    nodes.put(nodeJoins.id, nodeJoins.newNode);
    computeQuorumConstants();
    // Say hello to the new node
    nodeJoins.newNode.tell(new NodeHello(id), getSelf());

    log("Node " + nodeJoins.id + " joined! -> N: " + N + ", R: " + R + ", W: " + W);
  }

  void onNodeHello (NodeHello nodeHello) {
    nodes.put(nodeHello.id, getSender());
    computeQuorumConstants();

    log("Hello from " + nodeHello.id + " -> N: " + N + ", R: " + R + ", W: " + W);
  }

  void onNodeLeaves (NodeLeaves nodeLeaves) {
    nodes.remove(nodeLeaves.id);
    computeQuorumConstants();

    log("Node " + nodeLeaves.id + " left! -> N: " + N + ", R: " + R + ", W: " + W);
  }

  void onCoordinatorGet (CoordinatorGet getRequest) throws InterruptedException {
    log("Coordinating: get(" + getRequest.key + ") ");
    for (Integer i : getRequestResponsibleNodes(getRequest.key, R)) System.out.println(i + " "); System.out.println();
    int reqId = reqCount++;
    pendingRequests.put(reqId, new PendingRequest<>(reqId, getSender(), new Quorum<String>(R)));
    multicast(new Get(reqId, getRequest.key), getRequestResponsibleNodes(getRequest.key, R));

    Thread.sleep(T);
    System.out.println("TTTTTTTTTTTT " + pendingRequests.get(reqId));
    // TODO send error if pendingRequests.get(reqId)
    // StoreValue v = store.get(getRequest.key);
    // sender().tell(new Feedback(v == null ? "null" : v.value), getSelf());
  }

  void onGet (Get getRequest) {
    log("Get(" + getRequest.key + ") from " + getSender().path().name());
    StoreValue v = store.get(getRequest.key);
    getSender().tell(new GetResponse(getRequest.reqId, v != null ? v.value : null, GetResponse.STATUS.OK), getSender());
  }

  void onGetResponse (GetResponse getResponse) {
    log("Response for Get #" + getResponse.reqId + " from " + getSender().path().name() + ": " + getResponse.value);
    PendingRequest<String> r = pendingRequests.get(getResponse.reqId);
    if (r != null) {
      r.quorum.inc(getResponse.value);
      if (r.quorum.reached()) {
        log("Read quorum reached for Get #" + getResponse.reqId);
        // TODO check majority for r.quorum.values
        r.client.tell(new GetResponse(getResponse.reqId, r.quorum.values.get(0), GetResponse.STATUS.OK), getSelf());
        pendingRequests.remove(getResponse.reqId);
      }
    } // TODO else something?
  }

  void onUpdate (Update updateRequest) {
    log("Update(" + updateRequest.key + ", " + updateRequest.value + ") from " + getSender().path().name());
    // TODO
  }

  void onUpdateResponse (UpdateResponse updateResponse) {
    // TODO
  }

  void onCoordinatorUpdate (CoordinatorUpdate updateRequest) {
    log("Coordinating: update(" + updateRequest.key + ", " + updateRequest.value + ")");
    multicast(new Update(reqCount++, updateRequest.key, updateRequest.value), getRequestResponsibleNodes(updateRequest.key, W));

    // TODO send/update only after quorum
    StoreValue oldItem = store.get(updateRequest.key);
    if (oldItem != null) {
      ++oldItem.version;
      oldItem.value = updateRequest.value;
    } else {
      store.put(updateRequest.key, new StoreValue(updateRequest.value));
    }
    sender().tell(new Feedback("Banananana"), getSelf());
    KeyValStoreSystem.storesMap.get(this.id).setValue(store.toString());
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(NodeJoins.class, this::onNodeJoins)
      .match(NodeHello.class, this::onNodeHello)
      .match(NodeLeaves.class, this::onNodeLeaves)
      .match(Get.class, this::onGet)
      .match(GetResponse.class, this::onGetResponse)
      .match(Update.class, this::onUpdate)
      .match(UpdateResponse.class, this::onUpdateResponse)
      .match(CoordinatorGet.class, this::onCoordinatorGet)
      .match(CoordinatorUpdate.class, this::onCoordinatorUpdate)
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
