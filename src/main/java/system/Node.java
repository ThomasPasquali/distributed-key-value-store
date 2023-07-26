package system;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Formatter;
import java.util.StringJoiner;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import javafx.application.Platform;

import system.PendingRequest.ACT;

public class Node extends AbstractActor {

  public static final int T = 2000;
  public static final int N = 3;
  public static final int R = 2;
  public static final int W = 2;
  
  private int reqCount, joinCount;
  private boolean crashed, recovering;
  private Map<Integer, PendingRequest.Request<StoreValue>> pendingRequests;

  protected int idNode;
  protected ActorRef bootNode;
  protected Map<Integer, ActorRef> nodes;
  protected Map<Integer, StoreValue> store; 

  /* -------------------------------------- PUBLIC MESSAGE INTERFACE (CLIENT-NODE) ---------------------------------------- */
  
  public static class NodeLeave implements Serializable {}

  public static class Get implements Serializable { 
    public final int key;
    public Get(int key) {
      this.key = key;
    }
  }

  public static class Update implements Serializable { 
    public final int key;
    public final String value;
    public Update(int key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  public static enum STATUS { OK,  ERROR };
  public static class Feedback implements Serializable {
    public final String feedback;
    public Feedback(int reqId, StoreValue value, STATUS status, ACT act) {
      String str = "FEEDBACK: " + act + " #" + reqId + " [" + status + "]";
      if (value != null) { str += " | " + value; }
      this.feedback = str;
    }
  }

  public static class Crash implements Serializable {}

  public static class Recovery implements Serializable {
    public final ActorRef recoveryNode;
    public Recovery(ActorRef recoveryNode) {
      this.recoveryNode = recoveryNode;
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
    public final Map<Integer, StoreValue> items;
    public NodeGoodbye(int idSender, Map<Integer, StoreValue> items) {
      this.idSender = idSender;
      this.items = Collections.unmodifiableMap(new HashMap<>(items));
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
    public final Map<Integer, ActorRef> nodes;
    public GetNodesResponse(int idSender, Map<Integer, ActorRef> nodes) {
      this.idSender = idSender;
      this.nodes = Collections.unmodifiableMap(new HashMap<>(nodes));
    }
  }

  private static class GetItems implements Serializable {
    public final int idSender;
    public GetItems(int idSender) {
      this.idSender = idSender;
    }
  }

  private static class GetItemsResponse implements Serializable {
    public final Map<Integer, StoreValue> items;
    public GetItemsResponse(Map<Integer, StoreValue> items) {
      this.items = Collections.unmodifiableMap(new HashMap<>(items));
    }
  }

  private static class GetItem implements Serializable {
    public final int reqId, key;
    public GetItem(int reqId, int key) {
      this.reqId = reqId;
      this.key = key;
    }
  }

  private static class GetItemResponse implements Serializable {
    public final int reqId;
    public final StoreValue value;
    public GetItemResponse(int reqId, StoreValue value) {
      this.reqId = reqId;
      this.value = value;
    }
  }

  private static class UpdateItem implements Serializable {
    public final int key;
    public final StoreValue value;
    public UpdateItem(int key, StoreValue value) {
      this.key = key;
      this.value = value;
    }
  }  

  /* -------------------------------------- CLASS ---------------------------------------- */

  public Node(int id, ActorRef bootNode) {
    this.idNode = id;
    this.bootNode = bootNode;
    this.reqCount = 0;
    this.joinCount = 0;
    this.crashed = false;
    this.recovering = false;
    this.pendingRequests = new HashMap<>();
    this.nodes = new HashMap<>();
    this.store = new HashMap<>() {
      @Override
      public StoreValue put (Integer key, StoreValue value) {
        StoreValue res = super.put(key, value);
        updateStoreUI();
        return res;
      }
      @Override
      public StoreValue remove (Object key) {
        StoreValue res = super.remove(key);
        updateStoreUI();
        return res;
      }
      @Override
      public String toString () {
        if (store.isEmpty()) {
          return "Empty store!!";
        }
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
  }

  public static Props props(int id, ActorRef bootNode) {
    return Props.create(Node.class, () -> new Node(id, bootNode));
  }

  private void log(String log, boolean ignoreUI) {
    String timestamp = new SimpleDateFormat("HH:mm:ss.SS").format(new java.util.Date());
    System.out.println(timestamp + ": [Node_" + idNode + "] " + log);
    if (!ignoreUI) {
      Platform.runLater(new Runnable() {
        public void run() {
          KeyValStoreSystem.logsMap.get(idNode).add(log);
        }
      });
    }
  }
  
  private void log(String log) {
    log(log, false);
  }

  private void updateStoreUI() {
    Platform.runLater(new Runnable() {
      public void run() {
        if (crashed) {
          KeyValStoreSystem.storesMap.get(idNode).setValue("CRASHED!");
        } else {
          KeyValStoreSystem.storesMap.get(idNode).setValue(store.toString());
        }
      }
    });
    if (!crashed) { log("\n" + store.toString(), true); }
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

  /**
   * Get all the items in the store that are responsibility of a given node
   * @param idNode The identifier of the node
   * @return A subset of the local store
   */
  private Map<Integer, StoreValue> getItemsByNode(int idNode) {
    Map<Integer, StoreValue> items = new HashMap<>();
    
    boolean joiningNode = false;
    if (nodes.get(idNode) == null) { 
      nodes.put(idNode, null); 
      joiningNode = true;
    }

    for (Integer key : store.keySet()) {
      if (getInvolvedNodes(key, true).contains(idNode)) {
        items.put(key, store.get(key));
      }
    }

    if (joiningNode) { nodes.remove(idNode); }

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

  @Override
  public void preStart() {
    if (bootNode != null) {
      bootNode.tell(new Node.GetNodes(idNode), getSelf());
    }
  }
  
  void onGetNodes (GetNodes msg) {
    log("Node " + msg.idNew + " requested the list of nodes in the system");
    getSender().tell(new GetNodesResponse(idNode, nodes), getSelf());
  }
  
  void onGetNodesResponse (GetNodesResponse msg) {
    log("Received list of nodes from " + getSender().path().name());
    nodes.putAll(msg.nodes);
    nodes.put(msg.idSender, getSender());

    // If recovering remove items no longer responsible for
    if (recovering) {
      nodes.remove(idNode);
      for (Integer key : store.keySet()) {
        if (!getInvolvedNodes(key,  true).contains(idNode)) {
          store.remove(key);
        }
      }
    } 

    // Create join/recovering pending request
    int reqId = joinCount;
    PendingRequest.Request<StoreValue> req = new PendingRequest.Join<>(reqId, new PendingRequest.Quorum<StoreValue>(Math.min(nodes.size(), R)));
    pendingRequests.put(reqId, req);

    multicast(new GetItems(idNode), getInvolvedNodes(idNode, false));   
    
    Utils.setTimeout(() -> {
      PendingRequest.Request<StoreValue> joinReq = pendingRequests.get(reqId);
      // Check if the request is still pending after timeout
      if (joinReq != null) { 
        multicast(new NodeHello(this.idNode));
        pendingRequests.remove(joinCount++);
      }
    }, T);
  }

  void onGetItems (GetItems msg) {
    log("Node " + msg.idSender + " requested items");
    getSender().tell(new GetItemsResponse(getItemsByNode(msg.idSender)), getSelf());
  }

  void onGetItemsResponse (GetItemsResponse msg) {
    log("Received " + msg.items + " from " + getSender().path().name());

    // Get join/recovering pending request
    PendingRequest.Request<StoreValue> req = pendingRequests.get(joinCount); // Get pending request from id
    if (req == null) { return; }

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

    req.quorum.inc(null); // Increment the quorum

    // Exit if the quorum has not been reached yet
    if (req.quorum.reached()) { 
      // If not recovering (join operation) broadcasts Hello msg from the new node
      if (!recovering) { 
        multicast(new NodeHello(this.idNode)); 
      } else {
        crashed = false;
        recovering = false;
      }
      pendingRequests.remove(joinCount++);
    }
    updateStoreUI();
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
  }

  void onNodeLeave (NodeLeave msg) {
    log("Requested to leave");
    Map<Integer, Map<Integer, StoreValue>> toSend = new HashMap<>();
    
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
      Map<Integer, StoreValue> items = toSend.get(id);
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

  void onGetItem (GetItem msg) {
    log("GET(" + msg.key + ") from " + getSender().path().name());
    StoreValue value = store.get(msg.key); // Try to get value from the store
    
    // Send value to the sender
    ActorRef peer = getSender(), self = getSelf();
    Utils.setTimeout(() -> { // Add artificial delay
      peer.tell(new GetItemResponse(msg.reqId, value == null ? new StoreValue(null, -1) : value), self);
    }, KeyValStoreSystem.delaysMap.get(idNode).get());
  }

  void onUpdateItem (UpdateItem msg) {
    log("UPDATE(" + msg.key + ", " + msg.value + ") from " + getSender().path().name());
    store.put(msg.key, msg.value); // Update value in the store
  }  

  void onGet (Get msg) {
    log("Coordinating: GET(" + msg.key + ") ");

    // Create a pending GET Request
    int reqId = reqCount++;
    Set<Integer> involvedNodes = getInvolvedNodes(msg.key, true);
    PendingRequest.Request<StoreValue> req = new PendingRequest.Get<>(reqId, getSender(), new PendingRequest.Quorum<StoreValue>(R));
    
    // Check if the coordinator node should have the value
    if (involvedNodes.stream().anyMatch((nId) -> nId == idNode)) {
      StoreValue value = store.get(msg.key);
      req.quorum.inc(value == null ? new StoreValue(null, -1) : value); // Increment the quorum
    }

    pendingRequests.put(reqId, req); // Put the request in the container
    multicast(new GetItem(reqId, msg.key), involvedNodes); // Send a multicast to involved nodes
    setQueryTimeout(getSelf(), reqId); 
  }

  void onUpdate (Update msg) {
    log("Coordinating: UPDATE(" + msg.key + ", " + msg.value + ")");

    // Create a pending UPDATE Request
    int reqId = reqCount++;
    Set<Integer> involvedNodes = getInvolvedNodes(msg.key, true);
    PendingRequest.Update<StoreValue> req = new PendingRequest.Update<>(reqId, getSender(), new PendingRequest.Quorum<StoreValue>(W), msg.key, msg.value);
    req.setInvolvedNodes(involvedNodes, this.idNode);
    
    // Check if the coordinator node should have the value
    if (req.updateLocal) {
      StoreValue value = store.get(msg.key);
      req.quorum.inc(value == null ? new StoreValue(null, -1) : value);  // Increment the quorum
    }

    pendingRequests.put(reqId, req); // Put the request in the container
    multicast(new GetItem(reqId, msg.key), involvedNodes); // Send a multicast to involved nodes
    setQueryTimeout(getSelf(), reqId);
  }

  void onGetItemResponse (GetItemResponse msg) {
    String str_log = "Response for Get #" + msg.reqId + " from " + getSender().path().name() + ": " + msg.value;
    PendingRequest.Request<StoreValue> req = pendingRequests.get(msg.reqId); // Get pending request from id
    
    // Exit if the request has been already satisfied
    if (req == null) {
      log("[IGNORED] " + str_log);
      return;
    }

    log(str_log);
    req.quorum.inc(new StoreValue(msg.value.getValue(), msg.value.getVersion())); // Increment the quorum

    // Exit if the quorum has not been reached yet
    if (!req.quorum.reached()) { return; }

    log("Quorum reached for " + req.act + " #" + msg.reqId);
    
    // Get the fresher value from those received
    StoreValue freshValue = req.quorum.values.get(0);
    for (StoreValue v : req.quorum.values) {
      if (v.getVersion() > freshValue.getVersion()) {
        freshValue = v;
      }
    }

    // Give feedback to the client and remove pending request from the container
    req.client.tell(new Feedback(msg.reqId, req.act == ACT.GET ? freshValue : null, STATUS.OK, req.act), getSelf());
    pendingRequests.remove(msg.reqId);
    
    // Send update to the involved nodes 
    if (req.act == ACT.UPDATE) {
      PendingRequest.Update<StoreValue> updateReq = (PendingRequest.Update<StoreValue>) req; // Cast the request to update type
      StoreValue newValue = new StoreValue(updateReq.value, freshValue.getVersion() + 1); // Create new value
      if (updateReq.updateLocal) { store.put(updateReq.key, newValue); } // Update local value if required
      multicast(new UpdateItem(updateReq.key, newValue), updateReq.involvedNodes); // Send messages
    }
  }

  void onCrash(Crash msg) {
    crashed = true;
    getContext().become(crashed());
    log("Crashed!", false);
    updateStoreUI();    
  }

  void onRecovery(Recovery msg) {
    if (recovering) { return; }
    log("Recovering....");
    getContext().become(createReceive());
    recovering = true;
    msg.recoveryNode.tell(new GetNodes(idNode), getSelf());
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(NodeLeave.class, this::onNodeLeave)
      .match(Get.class, this::onGet)
      .match(Update.class, this::onUpdate)
      .match(NodeHello.class, this::onNodeHello)
      .match(NodeGoodbye.class, this::onNodeGoodbye)
      .match(GetNodes.class, this::onGetNodes)
      .match(GetNodesResponse.class, this::onGetNodesResponse)
      .match(GetItems.class, this::onGetItems)
      .match(GetItemsResponse.class, this::onGetItemsResponse)
      .match(GetItem.class, this::onGetItem)
      .match(GetItemResponse.class, this::onGetItemResponse)
      .match(UpdateItem.class, this::onUpdateItem) 
      .match(Crash.class, this::onCrash) 
      .matchAny(msg -> {})
      .build();
  }

  public Receive crashed() {
    return receiveBuilder()
      .match(Recovery.class, this::onRecovery)
      .matchAny(msg -> {})
      .build();
  }
}
