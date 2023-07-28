package client;

import akka.actor.ActorRef;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.TextField;
import javafx.scene.text.Text;
import javafx.scene.text.TextFlow;
import system.KeyValStoreSystem;

public class ClientController {

  private void showErrorDialog (String error) {
    (new Alert(AlertType.ERROR, error, ButtonType.OK)).showAndWait();
  }

  private KeyValStoreSystem system;
  private ObservableList<String> feedbacks;
  private ActorRef clientActor;

  @FXML
  private TextField getKeyField;

  @FXML
  private TextFlow textFlow;

  @FXML
  private TextField updateKeyField;

  @FXML
  private TextField updateValueField;

  @FXML
  private ChoiceBox<Integer> coordinatorChoiceBox;

  @FXML
  void getKey(ActionEvent event) {
    String err = null;
    Integer coordinatorId = coordinatorChoiceBox.getValue();
    if (coordinatorId == null) {
      err = "Please select a coordinator";
    }
    Integer key = null;
    try {
      key = Integer.parseInt(getKeyField.getText());
      if (key < 0) throw new Exception();
    } catch (Exception e) {
      err = "Invalid key: must be a non negative integer";
    }
    
    if (err == null) {
      feedbacks.add("Requesting get(" + key + ") to node " + coordinatorId);
      system.get(clientActor, coordinatorId, key);
    } else {
      showErrorDialog(err);
    }
  }

  @FXML
  void updateKey(ActionEvent event) {
    String err = null;
    Integer coordinatorId = coordinatorChoiceBox.getValue();
    if (coordinatorId == null) {
      err = "Please select a coordinator";
    }
    Integer key = null;
    try {
      key = Integer.parseInt(updateKeyField.getText());
      if (key < 0) throw new Exception();
    } catch (Exception e) {
      err = "Invalid key: must be a non negative integer";
    }

    if (err == null) {
      feedbacks.add("Requesting update(" + key + ", " + updateValueField.getText() + ") to node " + coordinatorId);
      system.update(clientActor, coordinatorId, key, updateValueField.getText());
    } else {
      showErrorDialog(err);
    }
  }

  public ClientController (KeyValStoreSystem system, int id) {
    this.system = system;
    feedbacks = FXCollections.observableArrayList();
    feedbacks.addListener((ListChangeListener.Change<? extends String> c) -> {
      while (c.next()) {
        Platform.runLater(() -> { // TODO fix java.util.ConcurrentModificationException
          if (c.wasAdded()) {
            c.getAddedSubList().stream().forEachOrdered(s -> textFlow.getChildren().add(new Text(s + "\n")));
          }
        });
      }
    });
    clientActor = system.createClientActor(id, feedbacks);
  }

  public ActorRef getClient() {
    return clientActor;
  }

  public void addNode (int id) {
    coordinatorChoiceBox.getItems().add(id);
    if (coordinatorChoiceBox.getItems().size() == 1) {
      coordinatorChoiceBox.setValue(id);
    }
  }

  public void removeNode (int id) {
    coordinatorChoiceBox.getItems().remove(coordinatorChoiceBox.getItems().indexOf(id));
    coordinatorChoiceBox.setValue(coordinatorChoiceBox.getItems().isEmpty() ? null : coordinatorChoiceBox.getItems().get(0));
  }

}
