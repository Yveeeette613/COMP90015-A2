import Interface.IBroker;
import Interface.IDirectory;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Broker extends UnicastRemoteObject implements IBroker {

    private String brokerIP;
    private int brokerPort;
    private IDirectory directoryService;
    private List<String> connectedPublishers;
    private List<String> connectedSubscribers;

    protected Broker(String brokerIP, int brokerPort, IDirectory directoryService) throws RemoteException {
        this.brokerIP = brokerIP;
        this.brokerPort = brokerPort;
        this.directoryService = directoryService;
        this.connectedPublishers = new ArrayList<>();
        this.connectedSubscribers = new ArrayList<>();
    }

    @Override
    public void forwardMessage(String topic, String message) throws RemoteException {
        System.out.println("Message received on topic '" + topic + "': " + message);
    }

    @Override
    public void notify(String message) throws RemoteException {
        System.out.println("Notification: " + message);
    }

    @Override
    public int getConnectedPublishers() throws RemoteException {
        return connectedPublishers.size();
    }

    @Override
    public void addPublisher(String name) throws RemoteException {
        connectedPublishers.add(name);
    }

    @Override
    public void removePublisher(String name) throws RemoteException {
        connectedPublishers.remove(name);
    }

    @Override
    public int getConnectedSubscribers() throws RemoteException {
        return connectedSubscribers.size();
    }

    @Override
    public void addSubscriber(String name) throws RemoteException {
        connectedSubscribers.add(name);
    }

    @Override
    public void removeSubscriber(String name) throws RemoteException {
        connectedSubscribers.remove(name);
    }

    public void registerAndConnect() throws Exception {
        directoryService.registerBroker(brokerIP, brokerPort);
        List<String> otherBrokers = directoryService.getActiveBrokers(brokerIP, brokerPort);

        for (String brokerAddress : otherBrokers) {
            try {
                IBroker otherBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
                System.out.println("Connected to broker: " + brokerAddress);

                // Notify existing brokers about the new connection
                otherBroker.notify("New broker connected: " + brokerIP + ":" + brokerPort);

            } catch (Exception e) {
                System.out.println("Failed to connect to broker: " + brokerAddress);
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        try {
            Scanner scanner = new Scanner(System.in);

            System.out.print("Enter the broker IP: ");
            String brokerIP = scanner.nextLine();

            System.out.print("Enter the broker port: ");
            int brokerPort = Integer.parseInt(scanner.nextLine());

            IDirectory directoryService = (IDirectory) Naming.lookup("//localhost:1099/DirectoryService");

            Broker broker = new Broker(brokerIP, brokerPort, directoryService);

            java.rmi.registry.LocateRegistry.createRegistry(brokerPort);
            Naming.rebind("//" + brokerIP + ":" + brokerPort + "/Broker", broker);
            System.out.println("Broker is running on " + brokerIP + ":" + brokerPort);

            broker.registerAndConnect();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}