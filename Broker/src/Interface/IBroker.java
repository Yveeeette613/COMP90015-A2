package Interface;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IBroker extends Remote {
    // Forward message to other brokers
    void forwardMessage(String topic, String message) throws RemoteException;

    // Notify the broker of an event
    void notify(String message) throws RemoteException;
}
