package main;

import java.io.IOException;
import java.util.Arrays;
import java.util.TreeSet;

import client.ClientController;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.Label;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import system.KeyValStoreSystem;

public class Main extends Application {

  static int clientCount = 0;
  private KeyValStoreSystem system;
  protected KeyValStoreSystem getSystem() {
    return system;
  }

  private void showErrorDialog (String error) {
    (new Alert(AlertType.ERROR, error, ButtonType.OK)).showAndWait();
  }

  protected Stage mainStage;

  @FXML
  private TextField newNodeIdField;

  @FXML
  private VBox nodesPane;

  @FXML
  void newNode(ActionEvent event) {
    try {
      newNode(Integer.parseInt(newNodeIdField.getText()), -1);
    } catch (NumberFormatException e) {
      showErrorDialog("Please provide a non negative, unique ID");
    } catch (Exception e) {
      showErrorDialog(e.getMessage());
    }
  }
  
  protected void nodeLeaves (int nodeId) {
    system.nodeLeaves(nodeId);
    int i = 0;
    for (Integer id : new TreeSet<>(getSystem().getCurrentNodeIds())) {
      if (id > nodeId) break;
      ++i;
    }
    nodesPane.getChildren().remove(i);
  }

  protected void newNode (int nodeId, int bootNodeId) throws Exception {
    SimpleStringProperty nodeLogsString = new SimpleStringProperty();
    SimpleStringProperty nodeStoreString = new SimpleStringProperty();
    SimpleIntegerProperty nodeResponseDelay = new SimpleIntegerProperty(0);
    system.createNode(nodeId, bootNodeId, nodeLogsString, nodeStoreString, nodeResponseDelay);

    TextArea logsTa = new TextArea();
    logsTa.setEditable(false);
    logsTa.textProperty().bind(nodeLogsString);
    logsTa.textProperty().addListener((v, o, n) -> { // FIXME scroll to bottom
      // logsTa.selectPositionCaret(logsTa.getLength());
      // logsTa.setScrollTop(Double.MAX_VALUE);
      // logsTa.appendText("");
    });

    TextArea storeTa = new TextArea();
    storeTa.setEditable(false);
    storeTa.textProperty().bind(nodeStoreString); // FIXME scroll to bottom

    HBox headHbox = new HBox(50, new Label("Node: " + nodeId));
    headHbox.setAlignment(Pos.CENTER);
    HBox bodyHbox = new HBox(10, logsTa, storeTa);
    bodyHbox.setAlignment(Pos.CENTER);
    VBox vbox = new VBox(5, headHbox, bodyHbox);
    vbox.setAlignment(Pos.CENTER);
    Button b = new Button("Node " + nodeId + " leaves");
    b.setOnAction((ActionEvent e) -> {
      system.nodeLeaves(nodeId);
      nodesPane.getChildren().remove(vbox);
    });
    TextField delayField = new TextField("0");
    delayField.setOnKeyTyped((e) -> {
      try {
        nodeResponseDelay.setValue(Integer.parseInt(delayField.getText()));
      } catch (Exception ignored) { }
    });
    HBox delayHBox = new HBox(5, new Label("Response delay (ms): "), delayField);
    delayHBox.setAlignment(Pos.CENTER);

    headHbox.getChildren().addAll(b, delayHBox);
    int newNodeI = -1;
    for (Integer id : new TreeSet<>(getSystem().getCurrentNodeIds())) {
      if (id > nodeId) break;
      ++newNodeI;
    }
    if (newNodeI < 0 || newNodeI >= nodesPane.getChildren().size()) {
      nodesPane.getChildren().add(vbox);
    } else {
      nodesPane.getChildren().add(newNodeI, vbox);
    }
    newNodeIdField.clear();
  }

  protected void newClient () {
    newClient(null);
  }
  
  @FXML
  void newClient(ActionEvent event) {
    try {
      FXMLLoader fxmlLoader = new FXMLLoader(ClassLoader.getSystemClassLoader().getResource("client.fxml"));
      int clientId = ++clientCount;
      ClientController clientController = new ClientController(system, clientId);
      fxmlLoader.setController(clientController);
      Stage stage = new Stage();
      stage.setTitle("Client " + clientId);
      stage.setResizable(false);
      stage.setScene(new Scene((SplitPane) fxmlLoader.load()));
      stage.setX(1100);
      stage.setY(50);
      stage.show();

      system.addClient(clientId, clientController);
      stage.setOnCloseRequest((v) -> { system.removeClient(clientId);});
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void start(Stage stage) {
    try {
      FXMLLoader fxmlLoader = new FXMLLoader(ClassLoader.getSystemClassLoader().getResource("main.fxml"));
      fxmlLoader.setController(this);
      mainStage = stage;
      stage.setTitle("Distributed key value store");
      stage.setResizable(false);
      stage.setScene(new Scene((VBox) fxmlLoader.load()));
      stage.setOnCloseRequest((e) -> { Platform.exit(); System.exit(0); });
      stage.setX(100);
      stage.setY(30);
      stage.show();

      system = new KeyValStoreSystem();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public static void main(String[] args) {
    Application.launch(args);
  }
}
