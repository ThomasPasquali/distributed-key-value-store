package tests;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.control.Alert;
import javafx.scene.control.DialogEvent;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.ButtonBar.ButtonData;
import javafx.scene.control.ButtonType;
import javafx.stage.Modality;
import javafx.stage.Stage;

import akka.actor.ActorRef;

import main.Main;

public class CrashRecovery extends Main {

  private ActorRef client;
  private ButtonType buttonTypeCancel;
  private Alert alert;
  private int stepId = 0;
  private final String[] stepDescriptions = {
    "Node 20 crashes",
    "Node 25 joins",
    "Update a key \"owned\" by node 20",
    "Node 20 recovers",
    "Node 20 crashes (again)",
    "Node 5 joins",
    "Node 10 leaves",
    "Node 20 recovers",
    "Done!"
  };

  @Override
  public void start(Stage stage) {
    super.start(stage);
    try {
      // Create nodes
      int[] nodeIds = {10, 20, 30, 40};
      for (int id : nodeIds) {
        newNode(id, 10);
        Thread.sleep(200);
      }

      // Initialize stores
      ObservableList<String> feedbacks = FXCollections.observableArrayList();
      client = getSystem().createClientActor(100, feedbacks);
      getSystem().update(client, 10, 0, "ZERO_v0");
      getSystem().update(client, 20, 1, "ONE_v0");
      getSystem().update(client, 30, 2, "TWO_v0");

      // Initialize UI
      alert = new Alert(AlertType.INFORMATION);
      buttonTypeCancel = new ButtonType("Go!", ButtonData.CANCEL_CLOSE);
      alert.setX(mainStage.getX() + mainStage.getWidth() + 50);
      alert.setY(mainStage.getHeight() - 150);
      alert.setTitle("Crash/Recovery simulation");
      alert.setHeaderText("Step " + (stepId + 1) + "/" + stepDescriptions.length + ": " + stepDescriptions[stepId]);
      // alert.setContentText("Click NEXT to perform this action");
      alert.initModality(Modality.WINDOW_MODAL);
      alert.getButtonTypes().setAll(buttonTypeCancel);
      alert.setOnCloseRequest((DialogEvent e) -> {
        e.consume();
        nextStep();
      });
      alert.show();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void nextStep () {
    switch (stepId++) {
        case 0: // Node 20 crashes
          getSystem().crashNode(20);
          break;

        case 1: // Node 25 joins
          try { newNode(25, 20); } catch (Exception e1) { e1.printStackTrace(); }
          break;

        case 2: // Update a key "owned" by node 20
          getSystem().update(client, 10, 0, "UPDATED_ZERO");
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

        default: 
          Platform.exit();
          System.exit(0);

      }
    if (stepId < stepDescriptions.length) {
      alert.setHeaderText("Step " + (stepId + 1) + "/" + stepDescriptions.length + ": " + stepDescriptions[stepId]);
    } else {
      alert.setHeaderText("MISSING STEP DESCRIPTION");
    }
  }

  public static void main(String[] args) {
    Application.launch(args);
  }
}
