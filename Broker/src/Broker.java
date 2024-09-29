// Broker/src/Broker.java
import Interface.IBroker;
import Interface.IDirectory;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Broker extends UnicastRemoteObject implements IBroker {

    private String brokerIP;
    private int brokerPort;
    private IDirectory directoryService;
    private List<String> connectedPublishers;
    private List<String> connectedSubscribers;
    private Queue<String> messageQueue;

    protected Broker(String brokerIP, int brokerPort, IDirectory directoryService) throws RemoteException {
        this.brokerIP = brokerIP;
        this.brokerPort = brokerPort;
        this.directoryService = directoryService;
        this.connectedPublishers = new ArrayList<>();
        this.connectedSubscribers = new ArrayList<>();
        this.messageQueue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public synchronized void forwardMessage(String message, Set<String> visitedBrokers) throws RemoteException {
        if (visitedBrokers == null) {
            visitedBrokers = new HashSet<>();
        }

        // Add current broker to the set of visited brokers
        visitedBrokers.add(brokerIP + ":" + brokerPort);

        for (String subscriber : connectedSubscribers) {
            System.out.println("Forwarding message to subscriber: " + subscriber);
            messageQueue.add(message);
        }

        List<String> otherBrokers = directoryService.getActiveBrokers(brokerIP, brokerPort);
        for (String brokerAddress : otherBrokers) {
            if (!visitedBrokers.contains(brokerAddress)) {
                try {
                    IBroker otherBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
                    otherBroker.forwardMessage(message, visitedBrokers);
                } catch (Exception e) {
                    System.out.println("Failed to forward message to broker: " + brokerAddress);
                    e.printStackTrace();
                }
            }
        }
    }

    public synchronized void publishMessage(String message) throws RemoteException {
        String timestamp = new SimpleDateFormat("dd/MM HH:mm:ss").format(new Date());
        String formattedMessage = "[" + timestamp + "] " + message;
        System.out.println("Publishing message: " + formattedMessage);
        forwardMessage(formattedMessage, null);
    }



    @Override
    public String receiveMessage() throws RemoteException {
        return messageQueue.poll();
    }

    @Override
    public synchronized void notify(String message) throws RemoteException {
        System.out.println("Notification: " + message);
    }

    @Override
    public int getConnectedPublishers() throws RemoteException {
        return connectedPublishers.size();
    }

    @Override
    public synchronized void addPublisher(String name) throws RemoteException {
        connectedPublishers.add(name);
        System.out.println("New publisher connected: " + name);
    }

    @Override
    public synchronized void removePublisher(String name) throws RemoteException {
        connectedPublishers.remove(name);
        System.out.println("Publisher disconnected: " + name);
    }

    @Override
    public int getConnectedSubscribers() throws RemoteException {
        return connectedSubscribers.size();
    }

    @Override
    public synchronized void addSubscriber(String name) throws RemoteException {
        connectedSubscribers.add(name);
        System.out.println("New subscriber connected: " + name);
    }

    @Override
    public synchronized void removeSubscriber(String name) throws RemoteException {
        connectedSubscribers.remove(name);
        System.out.println("Subscriber disconnected: " + name);
    }

    public void disconnect() throws RemoteException {
        directoryService.removeBroker(brokerIP, brokerPort);
        System.out.println("Broker disconnected: " + brokerIP + ":" + brokerPort);
        List<String> otherBrokers = directoryService.getActiveBrokers(brokerIP, brokerPort);
        for (String brokerAddress : otherBrokers) {
            try {
                IBroker otherBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
                otherBroker.notify("Broker disconnected: " + brokerIP + ":" + brokerPort);
            } catch (Exception e) {
                System.out.println("Failed to connect to broker: " + brokerAddress);
                e.printStackTrace();
            }
        }
    }

    public void registerAndConnect() throws Exception {
        directoryService.registerBroker(brokerIP, brokerPort);
        List<String> otherBrokers = directoryService.getActiveBrokers(brokerIP, brokerPort);

        for (String brokerAddress : otherBrokers) {
            try {
                IBroker otherBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
                System.out.println("Connected to broker: " + brokerAddress);
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

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    broker.disconnect();
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            }));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}