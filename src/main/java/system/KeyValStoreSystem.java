package system;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import client.ClientController;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.ObservableList;

public class KeyValStoreSystem {

  public static final HashMap<Integer, List<String>> logsMap = new HashMap<>();
  public static final HashMap<Integer, SimpleStringProperty> storesMap = new HashMap<>();
  public static final HashMap<Integer, SimpleIntegerProperty> delaysMap = new HashMap<>();

  private final ActorSystem system;
  private final HashMap<Integer, ActorRef> nodes = new HashMap<>();
  private final HashMap<Integer,ClientController> clientControllers = new HashMap<>();

  public KeyValStoreSystem () {
    system = ActorSystem.create("distr-key-val-system");
  }

  public Set<Integer> getCurrentNodeIds () {
    return nodes.keySet();
  }

  public class ClientActor extends AbstractActor {
    private final int id;
    ObservableList<String> feedbacks;
    public ClientActor (int id, ObservableList<String> feedbacks) {
      this.id = id;
      this.feedbacks = feedbacks;
    }
    void onFeedback (Node.Feedback feedback) {
      feedbacks.add(feedback.feedback);
    }
    @Override
    public Receive createReceive() {
      return receiveBuilder()
        .match(Node.Feedback.class, this::onFeedback)
        .build();
    }
  }

  public ActorRef createClientActor (int id, ObservableList<String> feedbacks) {
    return system.actorOf(Props.create(ClientActor.class, () -> new ClientActor(id, feedbacks)), "Client_" + id);
  }

  public void addClient (int id, ClientController clientController) {
    clientControllers.put(id, clientController);
    for (Integer i : nodes.keySet()) {
      clientController.addNode(i);
    }
  }

  public void removeClient (int id) {
    clientControllers.remove(id);
    for (ClientController c : clientControllers.values()) {
      c.removeNode(id);
    }
  }

  public void createNode (int id, SimpleStringProperty logsProp, SimpleStringProperty storeProp, SimpleIntegerProperty delayProp) throws Exception {
    // Check id
    if (id < 0) {
      throw new Exception("ID must be greater than zero");
    }
    if (nodes.containsKey(id)) {
      throw new Exception("Duplicate ID " + id);
    }

    // Binding node logs to TextArea
    logsMap.put(id, new ArrayList<String>() {
      @Override
      public boolean add (String s) {
        boolean res = super.add(s);
        if (res) {
          logsProp.setValue(String.join("\n", this));
        }
        return res;
      }
    });

    // Binding node stores to TextArea
    storesMap.put(id, storeProp);
    delaysMap.put(id, delayProp);    
    
    // TODO choose bootstrap node?
    ActorRef bootPeer = null;
    Optional<Integer> firstKey = nodes.keySet().stream().findFirst();
    if (firstKey.isPresent()) {
      bootPeer = nodes.get(firstKey.get());
    }

    // Creating Actor (Node)
    ActorRef node = system.actorOf(Node.props(id, bootPeer), "Node_" + id);

    nodes.put(id, node);
    for (ClientController c : clientControllers.values()) {
      c.addNode(id);
    }
  }

  public void nodeLeaves (int id) {
    ActorRef leavingNode = nodes.get(id);
    leavingNode.tell(new Node.NodeLeave(), ActorRef.noSender());
    system.stop(leavingNode);
    nodes.remove(id);
    removeClient(id);
  }

  public void get (ActorRef clientActor, int coordinatorId, int key) {
    nodes.get(coordinatorId).tell(new Node.Get(key), clientActor);
  }
  
  public void update (ActorRef clientActor, int coordinatorId, int key, String value) {
    nodes.get(coordinatorId).tell(new Node.Update(key, value), clientActor);
  }

  public void crashNode (int nodeId) {
    nodes.get(nodeId).tell(new Node.Crash(), null);
  }

  public void recoverNode (int nodeId) {
    ActorRef recoveryNode = null;
    for (Integer id : nodes.keySet()) {
      if (id != nodeId) {
        recoveryNode = nodes.get(id);
        break;
      }
    }
    nodes.get(nodeId).tell(new Node.Recovery(recoveryNode), null); // recoveryNode may be null...
  }
}