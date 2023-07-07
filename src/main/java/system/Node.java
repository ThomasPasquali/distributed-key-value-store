package system;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class Node extends AbstractActor {

  protected int id;                       
  protected List<ActorRef> nodes = new ArrayList<>();

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

  static public Props props(int id) {
    return Props.create(Node.class, () -> new Node(id));
  }

  public Node(int id) {
    this.id = id;
  }

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
  } */

  // emulate a delay of d milliseconds
  void delay(int d) {
    try {Thread.sleep(d);} catch (Exception ignored) {}
  }

  void multicast(Serializable m) {
    for (ActorRef p: nodes)
      p.tell(m, getSelf());
  }

  /*/ a multicast implementation that crashes after sending the first message
  void multicastAndCrash(Serializable m, int recoverIn) {
    for (ActorRef p: participants) {
      p.tell(m, getSelf());
      crash(recoverIn); return;
    }
  } */

  /*/ schedule a Timeout message in specified time
  void setTimeout(int time) {
    getContext().system().scheduler().scheduleOnce(
        Duration.create(time, TimeUnit.MILLISECONDS),  
        getSelf(),
        new Timeout(), // the message to send
        getContext().system().dispatcher(), getSelf()
        );
  } */
  void onNodeJoins (NodeJoins nodeJoins) {
    this.nodes.add(nodeJoins.newNode);

    String log = "Node " + nodeJoins.id + " joined!";
    System.out.println(log);
    KeyValStoreSystem.logsMap.get(this.id).add(log);
  }

  void onNodeLeaves (NodeLeaves nodeLeaves) {
    // TODO this.nodes.add(nodeJoins.newNode);

    String log = "Node " + nodeLeaves.id + " left!";
    System.out.println(log);
    KeyValStoreSystem.logsMap.get(this.id).add(log);
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(NodeJoins.class, this::onNodeJoins)
      .match(NodeLeaves.class, this::onNodeLeaves)
      .build();
  }

  /* public Receive crashed() {
    return receiveBuilder()
      .match(Recovery.class, this::onRecovery)
      .matchAny(msg -> {})
      .build();
  } */
}
