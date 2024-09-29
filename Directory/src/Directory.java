import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import Interface.IDirectory;

public class Directory extends UnicastRemoteObject implements IDirectory {
    private List<String> activeBrokers;  // List of broker IP:port strings

    protected Directory() throws RemoteException {
        activeBrokers = new ArrayList<>();
    }

    @Override
    public synchronized void registerBroker(String brokerIP, int brokerPort) throws RemoteException {
        String brokerAddress = brokerIP + ":" + brokerPort;
        if (!activeBrokers.contains(brokerAddress)) {
            activeBrokers.add(brokerAddress);
            System.out.println("Broker registered: " + brokerAddress);
        }
    }

    @Override
    public synchronized void removeBroker(String brokerIP, int brokerPort) throws RemoteException {
        String brokerAddress = brokerIP + ":" + brokerPort;
        if (activeBrokers.contains(brokerAddress)) {
            activeBrokers.remove(brokerAddress);
            System.out.println("Broker removed: " + brokerAddress);
        }
    }

    @Override
    public synchronized List<String> getActiveBrokers(String newBrokerIP, int newBrokerPort) throws RemoteException {
        List<String> otherBrokers = new ArrayList<>();
        String newBrokerAddress = newBrokerIP + ":" + newBrokerPort;

        // Return the list of brokers, excluding the newly registered one
        for (String broker : activeBrokers) {
            if (!broker.equals(newBrokerAddress)) {
                otherBrokers.add(broker);
            }
        }
        return otherBrokers;
    }

    public static void main(String[] args) {
        try {
            java.rmi.registry.LocateRegistry.createRegistry(1099);
            Directory directoryService = new Directory();
            java.rmi.Naming.rebind("//localhost/DirectoryService", directoryService);
            System.out.println("Directory Service is running...");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}