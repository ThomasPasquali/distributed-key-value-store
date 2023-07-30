package system;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

  public static final Map<Integer, List<String>> logsMap = new HashMap<>();
  public static final Map<Integer, SimpleStringProperty> storesMap = new HashMap<>();
  public static final Map<Integer, SimpleIntegerProperty> delaysMap = new HashMap<>();

  private final ActorSystem system;
  private final Set<Integer> crashed;
  private final Map<Integer, ActorRef> nodes;
  private final Map<Integer,ClientController> clientControllers;

  public KeyValStoreSystem () {
    system = ActorSystem.create("distr-key-val-system");
    crashed = new HashSet<>();
    nodes = new HashMap<>();
    clientControllers = new HashMap<>();
  }

  public Set<Integer> getCurrentNodeIds () {
    return nodes.keySet();
  }

  public class ClientActor extends AbstractActor {
    //private final int id;
    ObservableList<String> feedbacks;
    public ClientActor (int id, ObservableList<String> feedbacks) {
      //this.id = id;
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

  public ActorRef getClient(int id) {
    return clientControllers.get(id).getClient();
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

  public void createNode (int id, int idBootNode, SimpleStringProperty logsProp, SimpleStringProperty storeProp, SimpleIntegerProperty delayProp) throws Exception {
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
    
    // Select boot node; if the id is not valid (crashed or absent) take the first node valid in the list
    ActorRef bootNode = nodes.get(idBootNode);
    if (!nodes.isEmpty() && (bootNode == null || crashed.contains(idBootNode))) {
      for (Integer node : nodes.keySet()) {
        if (!crashed.contains(node)) {
          bootNode = nodes.get(node);
          break;
        }
      }
      if (bootNode == null) { throw new Exception("Node "+ id +" cannot join the system"); }
    }

    // Creating Actor (Node)
    ActorRef node = system.actorOf(Node.props(id, bootNode), "Node_" + id);

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
    crashed.add(nodeId);
  }

  public void recoverNode (int nodeId, int idRecoveryNode) throws Exception {
    // Select recovery node; if the id is not valid (crashed or absent) take the first node valid in the list
    ActorRef recoveryNode = nodes.get(idRecoveryNode);
    if (!nodes.isEmpty() && (recoveryNode == null || crashed.contains(idRecoveryNode))) {
      for (Integer id : nodes.keySet()) {
        if (id != nodeId && !crashed.contains(id)) {
          recoveryNode = nodes.get(id);
          break;
        }
      }
      if (recoveryNode == null) { throw new Exception("Node " + nodeId +" cannot recover"); }
    }
    
    nodes.get(nodeId).tell(new Node.Recovery(recoveryNode), null);
    crashed.remove(nodeId);
  }
}