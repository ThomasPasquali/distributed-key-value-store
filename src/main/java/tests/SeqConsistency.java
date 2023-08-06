package tests;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.stage.Stage;

import system.Utils;

public class SeqConsistency extends Test {

  @Override
  public void start(Stage stage) {
    super.start(stage, "Sequential consistency simulation");
  }

  @Override
  public void nextStep() {
    switch (stepId) {
      case 0: // Concurrent update+get same key - different coordinators  TEST 1
        getSystem().update(clients.get(0), 2, 10, 1,  "CONCURRENT R/W 1");
        Platform.runLater(() -> Utils.setTimeout(() -> {
            getSystem().get(clients.get(1), 2, 40, 1);
            getSystem().get(clients.get(1), 2, 50, 1);
        }, 15));
        break;
      
      case 1: // Concurrent update+get same key - different coordinators  TEST 2
        Platform.runLater(() -> Utils.setTimeout(() -> {
          getSystem().update(clients.get(0), 1, 10, 1,  "CONCURRENT R/W 2");
        }, 1));
        getSystem().get(clients.get(0), 1, 20, 1);
        getSystem().get(clients.get(1), 2, 30, 1);
        Platform.runLater(() -> Utils.setTimeout(() -> {
          getSystem().get(clients.get(0), 1, 20, 1);
          getSystem().get(clients.get(1), 2, 30, 1);
        }, 3000));
        break;
        
      case 2: // Concurrent updates same key - different coordinators  TEST 1
          getSystem().update(clients.get(0), 1, 10, 1,  "VALUE_FROM_CLIENT_1");
          getSystem().update(clients.get(1), 2, 20, 1,  "VALUE_FROM_CLIENT_2");
        break;
      
      case 3: // Concurrent updates same key - different coordinators  TEST 2
        Platform.runLater(() -> Utils.setTimeout(() -> {
          getSystem().update(clients.get(0), 1, 10, 1,  "VALUE_FROM_CLIENT_1");
        }, 13));
        Platform.runLater(() -> Utils.setTimeout(() -> {
          getSystem().update(clients.get(1), 2, 20, 1,  "VALUE_FROM_CLIENT_2");
        }, 10));
        break;

      case 4: // Concurrent updates same key - different coordinators  TEST 3
        Platform.runLater(() -> Utils.setTimeout(() -> {
          getSystem().update(clients.get(0), 1, 10, 1,  "VALUE_FROM_CLIENT_1");
        }, 14));
        Platform.runLater(() -> Utils.setTimeout(() -> {
          getSystem().update(clients.get(1), 2, 20, 1,  "VALUE_FROM_CLIENT_2");
        }, 10));
        break;
      }
  }

  public static void main(String[] args) {
    String[] params = {"5",  // Number of nodes 
                       "2",  // Number of clients
                       // Steps labels
                       "Concurrent write and read: client 1 writes, client 2 reads from different coordinators, after 15ms", 
                       "Concurrent write and read: client 1 writes, 2 gets to different coordinators, after 3s, same two gets",
                       "Concurrent writes v1: client 1 then 2",
                       "Concurrent writes v2: client 1 delay 13ms, 2 delay 10",
                       "Concurrent writes v3: client 1 delay 14ms, 2 delay 10", 
                       "Done!"};
    Application.launch(params);
  }

}
