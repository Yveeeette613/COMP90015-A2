import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import Interface.IDirectory;

public class Directory extends UnicastRemoteObject implements IDirectory {
    private List<String> activeBrokers;  // List of broker IP:port strings

    protected Directory() throws RemoteException {
        activeBrokers = new ArrayList<>();
    }

    //Register Broker
    @Override
    public synchronized void registerBroker(String brokerIP, int brokerPort) throws RemoteException {
        String brokerAddress = brokerIP + ":" + brokerPort;
        if (!activeBrokers.contains(brokerAddress)) {
            activeBrokers.add(brokerAddress);
            System.out.println("Broker registered: " + brokerAddress);
        }
    }

    //Remove Broker
    @Override
    public synchronized void removeBroker(String brokerIP, int brokerPort) throws RemoteException {
        String brokerAddress = brokerIP + ":" + brokerPort;
        if (activeBrokers.contains(brokerAddress)) {
            activeBrokers.remove(brokerAddress);
            System.out.println("Broker removed: " + brokerAddress);
        }
    }

    //Get active broker in the directory
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
            Scanner scanner = new Scanner(System.in);
            String directoryIP = "";
            int directoryPort = 0;
            boolean validInput = false;

            while (!validInput) {
                System.out.println("Please enter the IP address and port number (format: IP port): ");
                String input = scanner.nextLine();
                String[] parts = input.split(" ");

                if (parts.length == 2) {
                    try {
                        directoryIP = parts[0];
                        directoryPort = Integer.parseInt(parts[1]);
                        validInput = true;
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid port number. Please enter a valid IP address and port number.");
                    }
                } else {
                    System.out.println("Invalid format. Please enter the IP address and port number in the format: IP port");
                }
            }

            java.rmi.registry.LocateRegistry.createRegistry(directoryPort);
            Directory directoryService = new Directory();
            java.rmi.Naming.rebind("//"+ directoryIP + ":" + directoryPort + "/DirectoryService", directoryService);
            System.out.println("Directory Service is running...");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}