/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package appserver.client;

import appserver.comm.Message;
import appserver.comm.MessageTypes;
import appserver.job.Job;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Properties;

import utils.PropertyHandler;

public class FibonacciClient extends Thread implements MessageTypes {
    String host = null;
    int port;
    int argument;
    Properties properties;

    public FibonacciClient(String serverPropertiesFile, int parameter) {
        try {
            // create properties object
            this.properties = new PropertyHandler(serverPropertiesFile);
            this.host = properties.getProperty("HOST");
            System.out.println("[PlusOneClient.PlusOneClient] Host: " + host);
            this.port = Integer.parseInt(properties.getProperty("PORT"));
            System.out.println("[PlusOneClient.PlusOneClient] Port: " + port);
            this.argument = parameter;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
        
    public void run() {
        try {
            // connect to server
            Socket server = new Socket(this.host, this.port);
            String classString = "appserver.job.impl.Fibonacci";

            // create job request message`
            Job job = new Job(classString, this.argument);
            Message message = new Message(JOB_REQUEST,job);

            // write job to server
            ObjectOutputStream writeToServer = new ObjectOutputStream(server.getOutputStream());
            writeToServer.writeObject(message);

            // read result back from server
            ObjectInputStream readFromServer = new ObjectInputStream(server.getInputStream());
            Integer result = (Integer)readFromServer.readObject();
            System.out.println("RESULT: " + result);
        } catch (Exception e) {
            System.err.println("[FibonacciClient.run] Error occurred");
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        for (int i = 10; i > 0; i--) {
            (new FibonacciClient("../../config/Server.properties", i)).start();
        }
    }
}
