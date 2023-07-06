package client;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.layout.StackPane;
import javafx.stage.Stage;

public class ClientController extends Application {

    @Override
    public void start(Stage stage) {
        Label l = new Label(getParameters().getNamed().keySet().toString());
        Scene scene = new Scene(new StackPane(l), 640, 480);
        stage.setScene(scene);
        stage.show();
    }

}
