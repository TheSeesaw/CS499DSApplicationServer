package appserver.server;

import appserver.comm.ConnectivityInfo;
import java.util.Hashtable;

/**
 *
 * @author Dr.-Ing. Wolf-Dieter Otte
 */
public class SatelliteManager {

    // (the one) hash table that contains the connectivity information of all satellite servers
    static private Hashtable<String, ConnectivityInfo> satellites = null;

    public SatelliteManager() {
        satellites = new Hashtable<String, ConnectivityInfo>();
    }

    public void registerSatellite(ConnectivityInfo satelliteInfo) {
        satellites.put(satelliteInfo.getName(), satelliteInfo);
    }

    public ConnectivityInfo getSatelliteForName(String satelliteName) {
        System.out.println("[SatelliteManager.getSatelliteForName] returning info for " + satelliteName);
        return satellites.get(satelliteName);
    }
    
/*
    public Enumeration getSatellites() {
        return satellites.elements();
    }

    
    public void showSatellites() {

        ConnectivityInfo satelliteInfo = null;
        Enumeration satelliteEnum = this.getSatellites();
        System.err.println("\n[ServerThread.run] registered satellites");
        while (satelliteEnum.hasMoreElements()) {
            satelliteInfo = (ConnectivityInfo) satelliteEnum.nextElement();
            System.out.println("Satellite IP  : " + satelliteInfo.getHost() + " Satellite port: " + satelliteInfo.getPort() + " Satellite name: " + satelliteInfo.getName());
        }
    }
*/
}
