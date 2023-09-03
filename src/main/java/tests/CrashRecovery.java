package tests;

import javafx.application.Application;
import javafx.stage.Stage;

public class CrashRecovery extends Test {

  public CrashRecovery () {
    super(1500);
  }

  @Override
  public void start(Stage stage) {
    super.start(stage, "Crash - recovery simulation");

    getSystem().update(clients.get(0), 1, 10, 0, "ZERO_v0");
    getSystem().update(clients.get(0), 1, 20, 11, "ONE_v0");
    getSystem().update(clients.get(0), 1, 30, 22, "TWO_v0");
    getSystem().update(clients.get(0), 1, 40, 33, "THREE_v0");
  }

  @Override
  public void nextStep () {
    switch (stepId) {
      case 0: // Node 20 crashes
        getSystem().crashNode(20);
        break;

      case 1: // Node 25 joins
        try { newNode(25, 20); } catch (Exception e1) { e1.printStackTrace(); }
        break;

      case 2: // Update a key "owned" by node 20
        getSystem().update(clients.get(0), 1, 10, 0, "UPDATED_ZERO");
        break;

      case 3: // Node 20 recovers
        try { getSystem().recoverNode(20, -1); } catch (Exception e1) { e1.printStackTrace(); }
        break;

      case 4: // Node 20 crashes (again)
        getSystem().crashNode(20);
        break;

      case 5: // Node 5 joins
        try { newNode(5, 10); } catch (Exception e1) { e1.printStackTrace(); }
        break;

      case 6: // Node 10 leaves
        nodeLeaves(10);
        break;

      case 7: // Node 20 recovers
        try { getSystem().recoverNode(20, -1); } catch (Exception e1) { e1.printStackTrace(); }
        break;
    }
  }

  public static void main(String[] args) {
    String[] params = {"4",  // Number of nodes 
                       "1",  // Number of clients
                       // Steps labels
                       "Node 20 crashes",
                       "Node 25 joins",
                       "Update a key \"owned\" by node 20",
                       "Node 20 recovers",
                       "Node 20 crashes (again)",
                       "Node 5 joins",
                       "Node 10 leaves",
                       "Node 20 recovers",
                       "Done!"};
    Application.launch(params);
  }
}
