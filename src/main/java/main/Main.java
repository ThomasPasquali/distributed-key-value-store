package main;

import java.io.IOException;

import client.ClientController;
import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.SplitPane;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

public class Main extends Application {

  static int clientCount = 0;

  @FXML
  private ScrollPane nodesPane;

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
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public static void main(String[] args) {
    Application.launch(args);
  }
}
