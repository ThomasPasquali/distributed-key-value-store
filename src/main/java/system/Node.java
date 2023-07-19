package system;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Formatter;
import java.util.StringJoiner;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import system.PendingRequest.ACT;

public class Node extends AbstractActor {

  public static final int T = 1000;
  public static final int N = 3;
  public static final int R = 2;
  public static final int W = 2;
  
  private int reqCount = 0, joinGetCount = 0;
  private HashMap<Integer, PendingRequest.Request<StoreValue>> pendingRequests = new HashMap<>();

  protected int idNode;
  protected HashMap<Integer, ActorRef> nodes = new HashMap<>();
  protected HashMap<Integer, StoreValue> store = new HashMap<>() {
    @Override
    public String toString () {
      StringJoiner sj = new StringJoiner("\n");
      for (Integer k : store.keySet()) {
        StoreValue v = store.get(k);
        Formatter fmt = new Formatter();
        sj.add(fmt.format("%-3d -> %s", k, v.toString()).toString());
        fmt.flush();
        fmt.close();
      }
      return sj.toString();
    }
  }; 

  /* -------------------------------------- PUBLIC MESSAGE INTERFACE (CLIENT-NODE) ---------------------------------------- */

  public static enum STATUS { OK,  ERROR };

  public static class NodeJoin implements Serializable {
    public final ActorRef bootNode;
    public NodeJoin(ActorRef bootNode) {
      this.bootNode = bootNode;
    }
  }
  
  public static class NodeLeave implements Serializable {}

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

  public static class Feedback implements Serializable {
    public final String feedback;
    public Feedback(int reqId, StoreValue value, STATUS status, ACT act) {
      String str = "FEEDBACK: " + act + " #" + reqId + " [" + status + "]";
      if (value != null) { str += " | " + value; }
      this.feedback = str;
    }
  }

  /* -------------------------------------- PRIVATE MESSAGE INTERFACE (NODE-NODE) ---------------------------------------- */
  
  private static class NodeHello implements Serializable {
    public final int idSender;
    public NodeHello(int idSender) {
      this.idSender = idSender;
    }
  }
  
  private static class NodeGoodbye implements Serializable {
    public final int idSender;
    public final HashMap<Integer, StoreValue> items;
    public NodeGoodbye(int idSender, HashMap<Integer, StoreValue> items) {
      this.idSender = idSender;
      this.items = new HashMap<>(items);
    }
  }

  private static class GetNodes implements Serializable {
    public final int idNew;
    public GetNodes(int idNew) {
      this.idNew = idNew;
    }
  }

  private static class GetNodesResponse implements Serializable {
    public final int idSender;
    public final HashMap<Integer, ActorRef> nodes;
    public GetNodesResponse(int idSender, HashMap<Integer, ActorRef> nodes) {
      this.idSender = idSender;
      this.nodes = new HashMap<>(nodes);
    }
  }

  private static class GetItems implements Serializable {
    public final int idSender;
    public GetItems(int idSender) {
      this.idSender = idSender;
    }
  }

  private static class GetItemsResponse implements Serializable {
    public final HashMap<Integer, StoreValue> items;
    public GetItemsResponse(HashMap<Integer, StoreValue> items) {
      this.items = new HashMap<>(items);
    }
  }

  private static class Get implements Serializable {
    public final int reqId, key;
    public Get(int reqId, int key) {
      this.reqId = reqId;
      this.key = key;
    }
  }

  private static class GetResponse implements Serializable {
    public final int reqId;
    public final StoreValue value;
    public GetResponse(int reqId, StoreValue value) {
      this.reqId = reqId;
      this.value = value;
    }
  }

  private static class Update implements Serializable {
    public final int key;
    public final StoreValue value;
    public Update(int key, StoreValue value) {
      this.key = key;
      this.value = value;
    }
  }  

  /* -------------------------------------- CLASS ---------------------------------------- */

  public Node(int id) {
    this.idNode = id;
  }

  public static Props props(int id) {
    return Props.create(Node.class, () -> new Node(id));
  }

  private void log(String log) {
    String timestamp = new SimpleDateFormat("HH:mm:ss.SS").format(new java.util.Date());
    System.out.println(timestamp + ": [Node_" + idNode + "] " + log);
    //KeyValStoreSystem.logsMap.get(this.idNode).add(log);
  }

  private void updateStoreUI() {
    //KeyValStoreSystem.storesMap.get(this.idNode).setValue(store.toString());
    log(store.toString());
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
  void multicast(Serializable m, Set<Integer> nodeIds) {
    for (Integer id: nodeIds) {
      ActorRef peer = nodes.get(id);
      if (peer != null) {
        peer.tell(m, getSelf());
      }
    }
  }

  /**
   * @param key Key of the involved item
   * @return A set of ids representing the nodes responsible of the item
   */
  private Set<Integer> getInvolvedNodes (int key, boolean self) { // TODO test me
    List<Integer> ids = new ArrayList<>(nodes.keySet());
    if (self) { ids.add(idNode); }
    Collections.sort(ids);
    
    int i, count = 0;
    boolean last = true;
    for (i = 0; i < ids.size(); ++i) {
      if (ids.get(i) > key) {
        last = false; break;
      }
    }
    
    if (last) { i--; }
    
    Set<Integer> res = new HashSet<>();
    while (count < Math.min(ids.size(), N)) {
      res.add(ids.get(i));
      count++;
      i = (i + 1) % ids.size();
    }
    
    return res;
  }

  private HashMap<Integer, StoreValue> getItemsByNode(int idNode) {
    HashMap<Integer, StoreValue> items = new HashMap<>();
    for (Integer key : store.keySet()) {
      if (key <= idNode) {
        items.put(key, store.get(key));
      }
    }
    return items;
  }

  /**
   * Sets a timeout for a request solving; in case the request is still pending gives to the client a feedback of failure
   * @param self   The coordinator node
   * @param reqId  The request ID
   */
  void setQueryTimeout (ActorRef self, int reqId) {
    Utils.setTimeout(() -> {
      PendingRequest.Request<StoreValue> req = pendingRequests.get(reqId);
      // Check if the request is still pending after timeout
      if (req != null) { 
        log("Timeout for "+ req.act + " #" + reqId);
        req.client.tell(new Feedback(reqId, null, STATUS.ERROR, req.act), self);
        pendingRequests.remove(reqId);
      }
    }, T);
  }

  /* -------------------------------------- MESSAGE HANDLERS ---------------------------------------- */

  void onNodeJoin (NodeJoin msg) { 
    msg.bootNode.tell(new Node.GetNodes(idNode), getSelf());
  }
  
  void onGetNodes (GetNodes msg) {
    log("Node " + msg.idNew + " requested to join");
    getSender().tell(new GetNodesResponse(idNode, nodes), getSelf());
  }
  
  void onGetNodesResponse (GetNodesResponse msg) {
    log("Received list of nodes from " + getSender().path().name());
    nodes.putAll(msg.nodes);
    nodes.put(msg.idSender, getSender());
    multicast(new GetItems(idNode), getInvolvedNodes(idNode, false));    
  }

  void onGetItems (GetItems msg) {
    log("Node " + msg.idSender + " requested items");
    getSender().tell(new GetItemsResponse(getItemsByNode(msg.idSender)), getSelf());
  }

  void onGetItemsResponse (GetItemsResponse msg) {
    log("Received items from " + getSender().path().name());

    // Update local store from received values
    if (store.size() == 0) { store.putAll(msg.items); }
    else {
      for (Integer key : msg.items.keySet()) {
        StoreValue locValue = store.get(key);
        StoreValue extValue = msg.items.get(key);
        // Check if the value stored is the last version
        if (locValue == null || locValue.compareTo(extValue) < 0) {
          store.put(key, extValue);
        }
      }
    }

    // Broadcast Hello msg from the new node
    if (++joinGetCount == Math.min(nodes.size(), N)) {
      multicast(new NodeHello(this.idNode)); 
      if(store.size() > 0) { updateStoreUI(); }
    }
  }

  void onNodeHello (NodeHello msg) {
    log("Node " + msg.idSender + " joined!");
    nodes.put(msg.idSender, getSender());

    // Remove items that the node is no longer responsible for
    for (Integer key : getItemsByNode(msg.idSender).keySet()) {
      Set<Integer> involvedNodes = getInvolvedNodes(key, true);
      if (!involvedNodes.stream().anyMatch((nId) -> nId == idNode)) {
        store.remove(key);
      }
    }
    updateStoreUI();
  }

  void onNodeLeave (NodeLeave msg) {
    log("Requested to leave");
    HashMap<Integer, HashMap<Integer, StoreValue>> toSend = new HashMap<>();
    
    // For each item in the store get the responsible node to which send the item 
    for (Integer key : store.keySet()) {
      Set<Integer> invNodesNow   = getInvolvedNodes(key, true);
      Set<Integer> invNodesAfter = getInvolvedNodes(key, false);
      invNodesAfter.removeAll(invNodesNow);
      if (!invNodesAfter.isEmpty()) {
        Integer id = invNodesAfter.iterator().next();
        if (toSend.get(id) == null) { toSend.put(id, new HashMap<>()); }
        toSend.get(id).put(key, store.get(key));
      }
    }
    
    // Send Goodbye msg attaching the items
    for (Integer id : nodes.keySet()) {
      HashMap<Integer, StoreValue> items = toSend.get(id);
      if (items == null) { items = new HashMap<>(); }
      nodes.get(id).tell(new NodeGoodbye(idNode, items), getSelf());
    }
  }

  void onNodeGoodbye (NodeGoodbye msg) {
    log("Received items from leaving Node " + msg.idSender);
    store.putAll(msg.items);
    nodes.remove(msg.idSender);
    updateStoreUI();
  }

  void onGet (Get getRequest) {
    log("GET(" + getRequest.key + ") from " + getSender().path().name());
    StoreValue value = store.get(getRequest.key); // Try to get value from the store
    
    // Send value to the sender
    ActorRef peer = getSender(), self = getSelf();
    Utils.setTimeout(() -> { // Add artificial delay
      peer.tell(new GetResponse(getRequest.reqId, value == null ? new StoreValue(null, -1) : value), self);
    }, KeyValStoreSystem.delaysMap.get(idNode).get());
  }

  void onUpdate (Update updateRequest) {
    log("UPDATE(" + updateRequest.key + ", " + updateRequest.value + ") from " + getSender().path().name());
    store.put(updateRequest.key, updateRequest.value); // Update value in the store
    updateStoreUI();
  }  

  void onCoordinatorGet (CoordinatorGet getRequest) {
    log("Coordinating: GET(" + getRequest.key + ") ");

    // Create a pending GET Request
    int reqId = reqCount++;
    Set<Integer> involvedNodes = getInvolvedNodes(getRequest.key, true);
    PendingRequest.Request<StoreValue> req = new PendingRequest.Get<>(reqId, getSender(), new PendingRequest.Quorum<StoreValue>(R));
    
    // Check if the coordinator node should have the value
    if (involvedNodes.stream().anyMatch((nId) -> nId == idNode)) {
      StoreValue value = store.get(getRequest.key);
      req.quorum.inc(value == null ? new StoreValue(null, -1) : value); // Increment the quorum
    }

    pendingRequests.put(reqId, req); // Put the request in the container
    multicast(new Get(reqId, getRequest.key), involvedNodes); // Send a multicast to involved nodes
    setQueryTimeout(getSelf(), reqId); 
  }

  void onCoordinatorUpdate (CoordinatorUpdate updateRequest) {
    log("Coordinating: UPDATE(" + updateRequest.key + ", " + updateRequest.value + ")");

    // Create a pending UPDATE Request
    int reqId = reqCount++;
    Set<Integer> involvedNodes = getInvolvedNodes(updateRequest.key, true);
    PendingRequest.Update<StoreValue> req = new PendingRequest.Update<>(reqId, getSender(), new PendingRequest.Quorum<StoreValue>(W), updateRequest.key, updateRequest.value);
    req.setInvolvedNodes(involvedNodes, this.idNode);
    
    // Check if the coordinator node should have the value
    if (req.updateLocal) {
      StoreValue value = store.get(updateRequest.key);
      req.quorum.inc(value == null ? new StoreValue(null, -1) : value);  // Increment the quorum
    }

    pendingRequests.put(reqId, req); // Put the request in the container
    multicast(new Get(reqId, updateRequest.key), involvedNodes); // Send a multicast to involved nodes
    setQueryTimeout(getSelf(), reqId);
  }

  void onGetResponse (GetResponse getResponse) {
    String str_log = "Response for Get #" + getResponse.reqId + " from " + getSender().path().name() + ": " + getResponse.value;
    PendingRequest.Request<StoreValue> req = pendingRequests.get(getResponse.reqId); // Get pending request from id
    
    // Exit if the request has been already satisfied
    if (req == null || req.quorum.reached()) {
      log("[IGNORED] " + str_log);
      return;
    }

    log(str_log);
    req.quorum.inc(new StoreValue(getResponse.value.getValue(), getResponse.value.getVersion())); // Increment the quorum

    // Exit if the quorum has not been reached yet
    if (!req.quorum.reached()) { return; }

    log("Quorum reached for " + req.act + " #" + getResponse.reqId);
    
    // Get the fresher value from those received
    StoreValue freshValue = req.quorum.values.get(0);
    for (StoreValue v : req.quorum.values) {
      if (v.getVersion() > freshValue.getVersion()) {
        freshValue = v;
      }
    }

    // Give feedback to the client and remove pending request from the container
    req.client.tell(new Feedback(getResponse.reqId, req.act == ACT.GET ? freshValue : null, STATUS.OK, req.act), getSelf());
    pendingRequests.remove(getResponse.reqId);
    
    // Send update to the involved nodes 
    if (req.act == ACT.UPDATE) {
      PendingRequest.Update<StoreValue> updateReq = (PendingRequest.Update<StoreValue>) req; // Cast the request to update type
      StoreValue newValue = new StoreValue(updateReq.value, freshValue.getVersion() + 1); // Create new value
      if (updateReq.updateLocal) { store.put(updateReq.key, newValue); } // Update local value if required
      multicast(new Update(updateReq.key, newValue), updateReq.involvedNodes); // Send messages
      updateStoreUI();
    }
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(NodeJoin.class, this::onNodeJoin)
      .match(NodeLeave.class, this::onNodeLeave)
      .match(CoordinatorGet.class, this::onCoordinatorGet)
      .match(CoordinatorUpdate.class, this::onCoordinatorUpdate)
      .match(NodeHello.class, this::onNodeHello)
      .match(NodeGoodbye.class, this::onNodeGoodbye)
      .match(GetNodes.class, this::onGetNodes)
      .match(GetNodesResponse.class, this::onGetNodesResponse)
      .match(GetItems.class, this::onGetItems)
      .match(GetItemsResponse.class, this::onGetItemsResponse)
      .match(Get.class, this::onGet)
      .match(GetResponse.class, this::onGetResponse)
      .match(Update.class, this::onUpdate)      
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
