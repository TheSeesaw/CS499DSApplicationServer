package appserver.satellite;

import appserver.job.Job;
import appserver.comm.ConnectivityInfo;
import appserver.job.UnknownToolException;
import appserver.comm.Message;
import static appserver.comm.MessageTypes.JOB_REQUEST;
import static appserver.comm.MessageTypes.REGISTER_SATELLITE;
import appserver.job.Tool;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;
import utils.PropertyHandler;

/**
 * Class [Satellite] Instances of this class represent computing nodes that execute jobs by
 * calling the callback method of tool a implementation, loading the tool's code dynamically over a network
 * or locally from the cache, if a tool got executed before.
 *
 * @author Dr.-Ing. Wolf-Dieter Otte
 */
public class Satellite extends Thread {

    private ConnectivityInfo satelliteInfo = new ConnectivityInfo();
    private ConnectivityInfo serverInfo = new ConnectivityInfo();
    private HTTPClassLoader classLoader = null;
    private Hashtable toolsCache = null;
    private ServerSocket satelliteSocket = null;

    public Satellite(String satellitePropertiesFile, String classLoaderPropertiesFile, String serverPropertiesFile) {

        // read this satellite's properties and populate satelliteInfo object,
        // which later on will be sent to the server
        try {
            PropertyHandler satelliteProps = new PropertyHandler(satellitePropertiesFile);
            this.satelliteInfo.setPort(Integer.parseInt(satelliteProps.getProperty("PORT")));
            this.satelliteInfo.setHost(satelliteProps.getProperty("HOST"));
            this.satelliteInfo.setName(satelliteProps.getProperty("NAME"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }


        // read properties of the application server and populate serverInfo object
        // other than satellites, the as doesn't have a human-readable name, so leave it out
        try {
            PropertyHandler appServerProps = new PropertyHandler(serverPropertiesFile);
            this.serverInfo.setPort(Integer.parseInt(appServerProps.getProperty("PORT")));
            this.serverInfo.setHost(appServerProps.getProperty("HOST"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }


        // read properties of the code server and create class loader
        // -------------------
        try {
            PropertyHandler codeServerProps = new PropertyHandler(classLoaderPropertiesFile);
            this.classLoader = new HTTPClassLoader(codeServerProps.getProperty("HOST"), Integer.parseInt(codeServerProps.getProperty("PORT")));
        } catch (Exception ex) {
            ex.printStackTrace();
        }


        // create tools cache
        // -------------------
        this.toolsCache = new Hashtable<String,Tool>();

    }

    @Override
    public void run() {

        // register this satellite with the SatelliteManager on the server
        // ---------------------------------------------------------------
        // create a register message with the satellite's info as the contents
        Message registerMsg = new Message(REGISTER_SATELLITE, this.satelliteInfo);

        // connect to the application server
        try {
            Socket appServer = new Socket(this.serverInfo.getHost(), this.serverInfo.getPort());
            ObjectOutputStream writeToAppServer = new ObjectOutputStream(appServer.getOutputStream());
            writeToAppServer.writeObject(registerMsg);
            writeToAppServer.flush();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        // create server socket
        // ---------------------------------------------------------------
        try {
            this.satelliteSocket = new ServerSocket(this.satelliteInfo.getPort());
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        // start taking job requests in a server loop
        // ---------------------------------------------------------------
        while(true) {
          System.out.println("[Server.run] Waiting for connections on Port #" + this.satelliteInfo.getPort());
            try {
                SatelliteThread sThread = new SatelliteThread(satelliteSocket.accept(), this);
                System.out.println("[Server.run] A connection to a client is established!");
                sThread.start();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    // inner helper class that is instanciated in above server loop and processes single job requests
    private class SatelliteThread extends Thread {

        Satellite satellite = null;
        Socket jobRequest = null;
        ObjectInputStream readFromNet = null;
        ObjectOutputStream writeToNet = null;
        Message message = null;

        SatelliteThread(Socket jobRequest, Satellite satellite) {
            this.jobRequest = jobRequest;
            this.satellite = satellite;
        }

        @Override
        public void run() {
            System.out.println("[SatelliteThread.run] Thread started.");
            // setting up object streams
            try {
                this.readFromNet = new ObjectInputStream(this.jobRequest.getInputStream());
                this.writeToNet = new ObjectOutputStream(this.jobRequest.getOutputStream());
                // reading message
                this.message = (Message)readFromNet.readObject();
                System.out.println("[SatelliteThread.run] " + Integer.toString(message.getType()));
            } catch (Exception ex) {
                ex.printStackTrace();
            }

            switch (message.getType()) {
                case JOB_REQUEST:
                    // processing job request
                    Job job = (Job)this.message.getContent();
                    Tool tool = null;
                    // retrieve tool
                    try {
                        tool = (Tool)satellite.getToolObject(job.getToolName());
                        // calculate result
                        int result = (Integer)tool.go(job.getParameters());
                        // write result back to client
                        this.writeToNet.writeObject(result);
                    } catch (Exception tool_ex) {
                        tool_ex.printStackTrace();
                    }
                    break;

                default:
                    System.err.println("[SatelliteThread.run] Warning: Message type not implemented");
            }
        }
    }

    /**
     * Aux method to get a tool object, given the fully qualified class string
     * If the tool has been used before, it is returned immediately out of the cache,
     * otherwise it is loaded dynamically
     */
    public Tool getToolObject(String toolClassString) throws UnknownToolException, ClassNotFoundException, InstantiationException, IllegalAccessException {

        Tool toolObject = null;

        toolObject = (Tool)this.toolsCache.get(toolClassString);
        if (toolObject == null) {
            // tool needs to be loaded
            Class toolClass = this.classLoader.loadClass(toolClassString);
            toolObject = (Tool)toolClass.newInstance();
            this.toolsCache.put(toolClassString, toolObject);
        } else {
            System.out.println(toolClassString + " already in cache.");
        }

        return toolObject;
    }

    public static void main(String[] args) {
        // start the satellite
        Satellite satellite = new Satellite(args[0], args[1], args[2]);
        satellite.run();
    }
}
