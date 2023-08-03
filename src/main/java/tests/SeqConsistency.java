package tests;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.stage.Stage;

import system.Utils;

public class SeqConsistency extends Test {

  @Override
  public void start(Stage stage) {
    super.start(stage, "Sequential consistency simulation");
    getSystem().update(clients.get(0), 1,  10, 1,  "a");
    getSystem().update(clients.get(0), 1, 20, 22, "b");
    getSystem().update(clients.get(1), 2, 30, 31, "c");
    getSystem().update(clients.get(1), 2, 40, 15, "d");
  }

  @Override
  public void nextStep() {
    switch (stepId) {
        case 0: // Concurrent updates same key - different coordinators
            getSystem().update(clients.get(0), 1, 10, 1,  "VALUE_FROM_CLIENT_1");
            getSystem().update(clients.get(1), 2, 20, 1,  "VALUE_FROM_CLIENT_2");
          break;
        
        case 1: // Concurrent updates same key - different coordinators
          Platform.runLater(() -> Utils.setTimeout(() -> {
            getSystem().update(clients.get(0), 1, 10, 1,  "VALUE_FROM_CLIENT_1");
          }, 13));
          Platform.runLater(() -> Utils.setTimeout(() -> {
            getSystem().update(clients.get(1), 2, 20, 1,  "VALUE_FROM_CLIENT_2");
          }, 10));
          break;

        case 2: // Concurrent updates same key - different coordinators
          Platform.runLater(() -> Utils.setTimeout(() -> {
            getSystem().update(clients.get(0), 1, 10, 1,  "VALUE_FROM_CLIENT_1");
          }, 14));
          Platform.runLater(() -> Utils.setTimeout(() -> {
            getSystem().update(clients.get(1), 2, 20, 1,  "VALUE_FROM_CLIENT_2");
          }, 10));
          break;

        case 3: // Concurrent update+get same key - different coordinators
          Platform.runLater(() -> Utils.setTimeout(() -> {
            getSystem().update(clients.get(0), 1, 10, 1,  "SIMULTANEOUS R/W");
          }, 1));
          getSystem().get(clients.get(0), 1, 20, 1);
          getSystem().get(clients.get(1), 2, 30, 1);
          Platform.runLater(() -> Utils.setTimeout(() -> {
            getSystem().get(clients.get(0), 1, 20, 1);
            getSystem().get(clients.get(1), 2, 30, 1);
          }, 3000));
          break;
      }
  }

  public static void main(String[] args) {
    String[] params = {"4",  // Number of nodes 
                       "2",  // Number of clients
                       // Steps labels
                       "Concurrent writes v1: client 1 then 2",
                       "Concurrent writes v2: client 1 delay 13ms, 2 delay 10",
                       "Concurrent writes v3: client 1 delay 14ms, 2 delay 10", 
                       "Concurrent write and read: client 1 writes, two gets to different coordinators, after 3s, same two gets", 
                       "Done!"};
    Application.launch(params);
  }

}
