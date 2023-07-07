package main;

import java.io.IOException;

import client.ClientController;
import javafx.application.Application;
import javafx.beans.property.SimpleStringProperty;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
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

  @FXML
  private TextField newNodeIdField;

  @FXML
  private VBox nodesVBox;

  @FXML
  void newNode(ActionEvent event) {
    int nodeId = Integer.parseInt(newNodeIdField.getText());

    SimpleStringProperty nodeLogsString = new SimpleStringProperty();
    TextArea ta = new TextArea();
    ta.setEditable(false);
    ta.textProperty().bind(nodeLogsString);

    HBox hBox = new HBox(50, new Label("Node: " + nodeId));
    VBox vbox = new VBox(5, hBox, ta);
    Button b = new Button("Node " + nodeId + " leaves");
    b.setOnAction((ActionEvent e) -> {
      system.nodeLeaves(nodeId);
      nodesVBox.getChildren().remove(vbox);
    });
    hBox.getChildren().add(b);
    nodesVBox.getChildren().add(vbox);

    system.createNode(nodeId, nodeLogsString);
  }


  @FXML
  void newClient(ActionEvent event) {
    try {
      FXMLLoader fxmlLoader = new FXMLLoader(ClassLoader.getSystemClassLoader().getResource("client.fxml"));
      fxmlLoader.setController(new ClientController());
      Stage stage = new Stage();
      stage.setTitle("Client " + (++clientCount));
      stage.setResizable(false);
      stage.setScene(new Scene((SplitPane) fxmlLoader.load()));
      stage.show();
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
