package system;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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

  public static class NodeLeaves implements Serializable {
    public final int id;
    public NodeLeaves(int id) {
      this.id = id;
    }
  }

  public static class Get implements Serializable {
    public final int key;
    public Get(int key) {
      this.key = key;
    }
  }
  public static class CoordinatorGet implements Serializable {
    public final int key;
    public CoordinatorGet(int key) {
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
  public static class CoordinatorUpdate implements Serializable {
    public final int key;
    public final String value;
    public CoordinatorUpdate(int key, String value) {
      this.key = key;
      this.value = value;
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

  static public Props props(int id) {
    return Props.create(Node.class, () -> new Node(id));
  }

  /* CLASS */

  protected int id;
  protected HashMap<Integer, ActorRef> nodes = new HashMap<>();
  protected HashMap<Integer, StoreValue> store = new HashMap<>();

  public Node(int id) {
    this.id = id;
  }

  private void log(String log) {
    System.out.println("[Node_" + id + "] " + log);
    KeyValStoreSystem.logsMap.get(this.id).add(log);
  }

  void multicast(Serializable m) {
    for (ActorRef p: nodes.values())
      p.tell(m, getSelf());
  }

  void onNodeJoins (NodeJoins nodeJoins) {
    this.nodes.put(nodeJoins.id, nodeJoins.newNode);
    log("Node " + nodeJoins.id + " joined!");
  }

  void onNodeLeaves (NodeLeaves nodeLeaves) {
    this.nodes.remove(nodeLeaves.id);
    log("Node " + nodeLeaves.id + " left!");
  }

  void onGet (Get getRequest) {
    // TODO
  }

  void onCoordinatorGet (CoordinatorGet getRequest) {
    System.out.println("Coooord get");
    sender().tell(new Feedback("Agagagagaga"), getSelf());
    log("Coordinating: get(" + getRequest.key + ")");
  }

  void onUpdate (Update updateRequest) {
    // TODO
  }

  void onCoordinatorUpdate (CoordinatorUpdate updateRequest) {
    System.out.println("Coooord update");
    sender().tell(new Feedback("Banananana"), getSelf());
    KeyValStoreSystem.storesMap.get(this.id).setValue("BUBBUBUBUBBU");
    log("Coordinating: update(" + updateRequest.key + ", " + updateRequest.value + ")");
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(NodeJoins.class, this::onNodeJoins)
      .match(NodeLeaves.class, this::onNodeLeaves)
      .match(Get.class, this::onGet)
      .match(Update.class, this::onUpdate)
      .match(CoordinatorGet.class, this::onCoordinatorGet)
      .match(CoordinatorUpdate.class, this::onCoordinatorUpdate)
      //.match(NodeLeaves.class, this::onNodeLeaves)
      //.match(NodeLeaves.class, this::onNodeLeaves)
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
