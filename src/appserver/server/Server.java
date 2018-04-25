package appserver.server;

import appserver.comm.Message;
import static appserver.comm.MessageTypes.JOB_REQUEST;
import static appserver.comm.MessageTypes.REGISTER_SATELLITE;
import appserver.comm.ConnectivityInfo;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import utils.PropertyHandler;

/**
 *
 * @author Dr.-Ing. Wolf-Dieter Otte
 */
public class Server {

    // Singleton objects - there is only one of them. For simplicity, this is not enforced though ...
    static SatelliteManager satelliteManager = null;
    static LoadManager loadManager = null;
    static ServerSocket serverSocket = null;
    static Properties properties = null;
    static String host = null;
    static int port;

    public Server(String serverPropertiesFile) {

        // create satellite manager and load manager
        Server.satelliteManager = new SatelliteManager();
        Server.loadManager = new LoadManager();

        // read server properties and create server socket
        try {
            Server.properties = new PropertyHandler(serverPropertiesFile);
            Server.host = properties.getProperty("HOST");
            System.out.println("[Server.Server] Host: " + Server.host);
            Server.port = Integer.parseInt(properties.getProperty("PORT"));
            System.out.println("[Server.Server] Port: " + Server.port);
            Server.serverSocket = new ServerSocket(Server.port);
            System.out.println("[Server.Server] Server Socket created on port: " + Server.port);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void run() {
    // serve clients in server loop ...
    // when a request comes in, a ServerThread object is spawned
        while (true) {
            System.out.println("[Server.run] Waiting for connections on Port #" + Server.port);
            try {
                ServerThread sThread = new ServerThread(serverSocket.accept());
                System.out.println("[Server.run] A connection to a client is established!");
                sThread.start();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    // objects of this helper class communicate with satellites or clients
    private class ServerThread extends Thread {

        Socket client = null;
        ObjectInputStream readFromNet = null;
        ObjectOutputStream writeToNet = null;
        Message message = null;

        private ServerThread(Socket client) {
            this.client = client;
        }

        @Override
        public void run() {
            // set up object streams and read message
            try {
                readFromNet = new ObjectInputStream(client.getInputStream());
                writeToNet = new ObjectOutputStream(client.getOutputStream());
                message = (Message)readFromNet.readObject();
            } catch (Exception ex) {
                ex.printStackTrace();
                System.out.println("FAILED TO GET MESSAGE");
            }
            // process message
            switch (message.getType()) {
                case REGISTER_SATELLITE:
                    // read satellite info
                    ConnectivityInfo incomingSatelliteInfo = (ConnectivityInfo)message.getContent();
                    // register satellite
                    synchronized (Server.satelliteManager) {
                        Server.satelliteManager.registerSatellite(incomingSatelliteInfo);
                        System.out.println("[ServerThread.run] Registered new satellite: " + incomingSatelliteInfo.getName());
                    }

                    // add satellite to loadManager
                    synchronized (Server.loadManager) {
                        Server.loadManager.satelliteAdded(incomingSatelliteInfo.getName());
                        System.out.println("[ServerThread.run] Added satellite to load manager: " + incomingSatelliteInfo.getName());
                    }

                    break;

                case JOB_REQUEST:
                    System.err.println("\n[ServerThread.run] Received job request");
                    ConnectivityInfo satelliteInfo = null;
                    String satelliteName = null;
                    synchronized (Server.loadManager) {
                        // get next satellite from load manager
                        try {
                            satelliteName = Server.loadManager.nextSatellite();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        // get connectivity info for next satellite from satellite manager
                        satelliteInfo = Server.satelliteManager.getSatelliteForName(satelliteName);
                    }

                    Socket satellite = null;
                    ObjectInputStream readFromSatellite = null;
                    ObjectOutputStream writeToSatellite = null;
                    System.out.println("[Server.run] Satellite host: " + satelliteInfo.getHost());
                    System.out.println("[Server.run] Satellite port: " + Integer.toString(satelliteInfo.getPort()));
                    try {
                        // connect to satellite
                        satellite = new Socket(satelliteInfo.getHost(), satelliteInfo.getPort());
                        // open object streams,
                        writeToSatellite = new ObjectOutputStream(satellite.getOutputStream());
                        readFromSatellite = new ObjectInputStream(satellite.getInputStream());

                        // forward message (as is) to satellite,
                        writeToSatellite.writeObject(message);
                        // receive result from satellite and
                        Object resultMsg = readFromSatellite.readObject();

                        // write result back to client
                        writeToNet.writeObject(resultMsg);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    break;

                default:
                    System.err.println("[ServerThread.run] Warning: Message type not implemented");
            }
        }
    }

    // main()
    public static void main(String[] args) {
        // start the application server
        Server server = null;
        if(args.length == 1) {
            server = new Server(args[0]);
        } else {
            server = new Server("../../config/Server.properties");
        }
        server.run();
    }
}
