package main;

import java.io.IOException;

import client.ClientController;
import javafx.application.Application;
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

  private void showErrorDialog (String error) {
    (new Alert(AlertType.ERROR, error, ButtonType.OK)).showAndWait();
  }

  @FXML
  private TextField newNodeIdField;

  @FXML
  private VBox nodesPane;

  @FXML
  void newNode(ActionEvent event) {
    try {
      newNode(Integer.parseInt(newNodeIdField.getText()));
    } catch (NumberFormatException e) {
      showErrorDialog("Please provide a non negative, unique ID");
    } catch (Exception e) {
      showErrorDialog(e.getMessage());
    }
  }

  void newNode (int nodeId) throws Exception {
    SimpleStringProperty nodeLogsString = new SimpleStringProperty();
    SimpleStringProperty nodeStoreString = new SimpleStringProperty();
    system.createNode(nodeId, nodeLogsString, nodeStoreString);

    TextArea logsTa = new TextArea();
    logsTa.setEditable(false);
    logsTa.textProperty().bind(nodeLogsString);
    logsTa.textProperty().addListener((v, o, n) -> { // FIXME scroll to bottom
      logsTa.setScrollTop(Double.MAX_VALUE);
      logsTa.appendText("");
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
    headHbox.getChildren().add(b);
    nodesPane.getChildren().add(vbox);
    newNodeIdField.clear();
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
      stage.setY(100);
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
      stage.setTitle("Distributed key value store");
      stage.setResizable(false);
      stage.setScene(new Scene((VBox) fxmlLoader.load()));
      stage.setX(50);
      stage.setY(100);
      stage.show();

      system = new KeyValStoreSystem();
      for (int i = 0; i < 70; i += 10) {
        newNode(i);
      }
      newClient(null);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public static void main(String[] args) {
    Application.launch(args);
  }
}
