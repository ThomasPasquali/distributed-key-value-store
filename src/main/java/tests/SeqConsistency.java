package tests;

import javafx.application.Application;
import javafx.stage.Stage;

public class SeqConsistency extends Test {

  @Override
  public void start(Stage stage) {
    super.start(stage);
   
    getSystem().update(clients.get(0), 10, 1,  "a1");
    getSystem().update(clients.get(0), 20, 22, "b1");
    getSystem().update(clients.get(1), 30, 31, "c1");
    getSystem().update(clients.get(1), 40, 15, "d1");
  }

  @Override
  public void nextStep() {
    switch (stepId) {
        case 0: // Concurrent updates same key - different coordinators
          getSystem().update(clients.get(0), 10, 1,  "a2");
          getSystem().update(clients.get(1), 20, 1,  "a3");
          break;

        case 1: // Concurrent update+get same key - different coordinators
          getSystem().update(clients.get(0), 10, 1,  "a4");
          getSystem().get(clients.get(0), 20, 1);
          getSystem().get(clients.get(1), 30, 1);
          super.sleep(500); 
          getSystem().get(clients.get(0), 20, 1);
          getSystem().get(clients.get(1), 30, 1);
          break;
      }
  }

  public static void main(String[] args) {
    String[] params = {"4",  // Number of nodes 
                       "2",  // Number of clients
                       // Steps labels
                       "Concurrent writes", 
                       "Concurrent write and read", 
                       "Done!"};
    Application.launch(params);
  }

}
