package Interface;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface IDirectory extends Remote {
    // Register a broker with IP address and port
    void registerBroker(String brokerIP, int brokerPort) throws RemoteException;

    // Retrieve a list of active brokers (excluding the newly registered broker)
    List<String> getActiveBrokers(String newBrokerIP, int newBrokerPort) throws RemoteException;

    // Remove a broker from the list of active brokers
    void removeBroker(String brokerIP, int brokerPort) throws RemoteException;
}