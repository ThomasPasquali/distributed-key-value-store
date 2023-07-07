package system;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringJoiner;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import javafx.beans.property.SimpleStringProperty;

public class KeyValStoreSystem {

  private final ActorSystem system;
  List<ActorRef> nodes = new ArrayList<>();
  public static final HashMap<Integer, List<String>> logsMap = new HashMap<>();

  public KeyValStoreSystem () {
    system = ActorSystem.create("distr-key-val-system");
  }

  public void createNode (int id, SimpleStringProperty logProp) {
    ArrayList<String> a = new ArrayList<>() {
      @Override
      public boolean add (String s) {
        boolean res = super.add(s);
        if (res) {
          logProp.setValue(String.join("\n", this));
        }
        return res;
      }
    };
    logsMap.put(id, a);

    ActorRef node = system.actorOf(Node.props(id), "Node_" + id);
    Node.NodeJoins nodeJoins = new Node.NodeJoins(node, id);
    for (ActorRef peer: nodes) {
      peer.tell(nodeJoins, null);
    }
    nodes.add(node);
  }

  public void nodeLeaves (int id) {
    Node.NodeLeaves nodeLeaves = new Node.NodeLeaves(id);
    for (ActorRef peer: nodes) {
      peer.tell(nodeLeaves, null);
    }
    // TODO nodes.remove
  }
  
}