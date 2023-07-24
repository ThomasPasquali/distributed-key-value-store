package tests;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.control.Alert;
import javafx.scene.control.DialogEvent;
import javafx.scene.control.Alert.AlertType;
import javafx.stage.Stage;

import akka.actor.ActorRef;

import main.Main;
import system.Utils;

public class CrashRecovery extends Main {

  @Override
  public void start(Stage stage) {
    super.start(stage);
    try {
      // Create nodes
      int[] nodeIds = {10, 20, 30, 40, 50};
      for (int id : nodeIds) {
        newNode(id);
        Thread.sleep(200);
      }

      // Initialize stores
      ObservableList<String> feedbacks = FXCollections.observableArrayList();
      ActorRef client = getSystem().createClientActor(100, feedbacks);
      getSystem().update(client, 10, 0, "ZERO_v0");
      getSystem().update(client, 20, 1, "ONE_v0");
      getSystem().update(client, 30, 2, "TWO_v0");

      Utils.setTimeout(() -> {
        getSystem().crashNode(10);
        getSystem().crashNode(20);
        Platform.runLater(() -> {
          Alert alert = new Alert(AlertType.INFORMATION);
          alert.setTitle("Crash simulation");
          alert.setHeaderText(null);
          alert.setContentText("Click OK to recover node 10!");
          alert.show();
          alert.setOnCloseRequest((DialogEvent e) -> {
            getSystem().recoverNode(10);
          });
        });
      }, 2000);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    Application.launch(args);
  }
}
