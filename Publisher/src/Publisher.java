import Interface.IBroker;
import Interface.IDirectory;
import java.rmi.Naming;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Publisher {
    private static String pubName;
    private static IBroker broker = null;

    public static void main(String[] args) {
        try {
            Scanner scanner = new Scanner(System.in);
            String directoryIP = "";
            int directoryPort = 0;
            boolean validInput = false;

            while (!validInput) {
                System.out.println("Please enter username directory_ip directory_port: ");
                String input = scanner.nextLine();
                String[] parts = input.split(" ");

                if (parts.length == 3) {
                    try {
                        pubName = parts[0];
                        directoryIP = parts[1];
                        directoryPort = Integer.parseInt(parts[2]);
                        validInput = true;
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid port number. Please enter a valid username, IP address, and port number.");
                    }
                } else {
                    System.out.println("Invalid format. Please enter the username, IP address, and port number in the format: username directory_ip directory_port");
                }
            }

            IDirectory directoryService = (IDirectory) Naming.lookup("//" + directoryIP + ":" + directoryPort + "/DirectoryService");
            // Get the list of active brokers
            List<String> activeBrokers = directoryService.getActiveBrokers("", 0);

//            System.out.println("Please enter your name: ");
//            pubName = scanner.nextLine();

            if (activeBrokers.isEmpty()) {
                System.out.println("No active brokers found.");
                return;
            }

            // Display the list of brokers to the user
            System.out.println("Active brokers:");
//            System.out.println("Please select brokers with less than 5 publishers.");

            for (int i = 0; i < activeBrokers.size(); i++) {
                String brokerAddress = activeBrokers.get(i);
//                IBroker tempBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
//                int connectedPublishers = tempBroker.getConnectedPublishers();
//                System.out.println((i + 1) + ". " + brokerAddress + " (Connected Publishers: " + connectedPublishers + ")");
                System.out.println((i + 1) + ". " + brokerAddress);
            }

            // Allow the user to select a broker
            System.out.print("Select a broker to connect to (1-" + activeBrokers.size() + "): ");
            int brokerIndex = Integer.parseInt(scanner.nextLine()) - 1;

            if (brokerIndex < 0 || brokerIndex >= activeBrokers.size()) {
                System.out.println("Invalid selection.");
                return;
            }

            String selectedBroker = activeBrokers.get(brokerIndex);
            String[] brokerParts = selectedBroker.split(":");
            String brokerIP = brokerParts[0];
            int brokerPort = Integer.parseInt(brokerParts[1]);

            // Connect to the selected broker
            broker = (IBroker) Naming.lookup("//" + brokerIP + ":" + brokerPort + "/Broker");

//            // Check if the broker can accept more publishers
//            if (broker.getConnectedPublishers() >= 5) {
//                System.out.println("Broker has reached the maximum number of connected publishers.");
//                return;
//            }

            System.out.println("Connected to broker " + brokerIP + ":" + brokerPort);
            broker.addPublisher(pubName);

            // Add shutdown hook to remove publisher when JVM shuts down
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (broker != null) {
                    try {
                        broker.removePublisher(pubName);
                        System.out.println("Disconnected from broker.");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }));

            // Publish messages
//            while (true) {
////                System.out.print("Enter the message to publish (or 'exit' to quit): ");
////                String message = scanner.nextLine();
////                if (message.equalsIgnoreCase("exit")) {
////                    break;
////                }
////                broker.publishMessage(message);
////                System.out.println("Message published.");
//
//                System.out.print("Enter the topic ID: ");
//                String topicID = scanner.nextLine();
//                System.out.print("Enter the topic Name: ");
//                String topicName = scanner.nextLine();
//                broker.createTopic(pubName, topicName, topicID);
//                System.out.println("Topic created.");
//
//            }

            while (true) {
                System.out.print("Enter command (publish/create/show/delete/exit): ");
                String command = scanner.nextLine();

                if (command.equalsIgnoreCase("publish")){
                    System.out.print("Enter the topic ID: ");
                    String topicID = scanner.nextLine();
                    System.out.print("Enter the message: ");
                    String message = scanner.nextLine();

                    broker.publishMessage(topicID, message);
                    System.out.println("Message published.");
                } else if (command.equalsIgnoreCase("create")) {
                    System.out.print("Enter the topic ID: ");
                    String topicID = scanner.nextLine();
                    System.out.print("Enter the topic Name: ");
                    String topicName = scanner.nextLine();
                    broker.createTopic(pubName, topicName, topicID);
                    System.out.println("Topic created.");
                } else if (command.equalsIgnoreCase("show")) {
                    List<String> topicList = broker.getPubTopicList(pubName);
                    for (String topic: topicList){
                        System.out.println(topic);
                    }
                } else if (command.equalsIgnoreCase("delete")) {
                    System.out.print("Enter the topic ID: ");
                    String topicId = scanner.nextLine();
                    broker.removeTopic(topicId);
                    System.out.println("Topic removed.");
                } else if (command.equalsIgnoreCase("exit")) {
                    break;
                } else{
                    System.out.println("Wrong Command, please enter again:)");
                }
            }


//            // send a message to the broker
//            System.out.print("Please select command: create, publish, show, delete.");
//            String topic = scanner.nextLine();

            // System.out.print("Enter the message: ");
            // String message = scanner.nextLine();
            //
            // broker.forwardMessage(topic, message);
            // System.out.println("Message sent to broker.");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (broker != null) {
                try {
                    broker.removePublisher(pubName);
                    System.out.println("Disconnected from broker.");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}