package Interface;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IBroker extends Remote {
    // Forward message to other brokers
    void forwardMessage(String topic, String message) throws RemoteException;

    // Notify the broker of an event
    void notify(String message) throws RemoteException;
    // Check the number of connected publishers
    int getConnectedPublishers() throws RemoteException;

    // Add a publisher
    void addPublisher(String name) throws RemoteException;

    // Remove a publisher
    void removePublisher(String name) throws RemoteException;

    // Check the number of connected Subscribers
    int getConnectedSubscribers() throws RemoteException;

    // Add a Subscriber
    void addSubscriber(String name) throws RemoteException;

    // Remove a Subscriber
    void removeSubscriber(String name) throws RemoteException;
}