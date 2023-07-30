package tests;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import akka.actor.ActorRef;
import javafx.application.Platform;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.ButtonBar.ButtonData;
import javafx.scene.control.ButtonType;
import javafx.scene.control.DialogEvent;
import javafx.stage.Modality;
import javafx.stage.Stage;
import main.Main;

public abstract class Test extends Main {

  protected int nNodes, nClients;
  protected Set<Integer> nodeIds;
  protected List<ActorRef> clients;
  
  protected int stepId = 0;
  protected List<String> stepDescriptions;
  protected Alert alert;

  @Override
  public void init() {
    List<String> params = getParameters().getRaw();
    this.nNodes = Integer.parseInt(params.get(0));
    this.nClients = Integer.parseInt(params.get(1));
    this.nodeIds = new HashSet<>(nNodes);
    this.clients = new ArrayList<>(nClients);
    this.stepDescriptions = params.subList(2, params.size());
  }

  public void sleep(int millis) {
    try {
      Thread.sleep(millis);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void start(Stage stage) {
    super.start(stage);
    
    try {
      for (int i = 0; i < nNodes; i++) {
        nodeIds.add((i + 1) * 10);
        newNode((i + 1) * 10, 10);
        Thread.sleep(200);
      }

      for (int i = 0; i < nClients; i++) {
        newClient();
        clients.add(getSystem().getClient(i + 1));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Initialize UI
    alert = new Alert(AlertType.INFORMATION);
    ButtonType buttonTypeCancel = new ButtonType("Go!", ButtonData.CANCEL_CLOSE);
    alert.setX(mainStage.getX() + mainStage.getWidth() + 50);
    alert.setY(mainStage.getHeight() - 150);
    alert.setTitle("Sequential consistency simulation");
    alert.setHeaderText("Step " + (stepId + 1) + "/" + stepDescriptions.size() + ": " + stepDescriptions.get(stepId));
    alert.initModality(Modality.WINDOW_MODAL);
    alert.getButtonTypes().setAll(buttonTypeCancel);
    alert.setOnCloseRequest((DialogEvent e) -> {
      e.consume();
      nextStep();
      stepId++;
      if (stepId < stepDescriptions.size()) {
        alert.setHeaderText("Step " + (stepId + 1) + "/" + stepDescriptions.size() + ": " + stepDescriptions.get(stepId));
      } else {
        Platform.exit();
        System.exit(0);
      }
    });
    alert.show();
  }    

  public abstract void nextStep();
}
