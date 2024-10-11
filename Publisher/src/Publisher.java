import Interface.IBroker;
import Interface.IDirectory;
import java.rmi.Naming;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Publisher {
    private static String pubName;
    private static IBroker broker = null;
    private static final String createTopicAction = "create";
    private static final String publishMessageAction = "publish";
    private static final String showExistedTopicAction = "show";
    private static final String deleteTopicAction = "delete";


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

            if (activeBrokers.isEmpty()) {
                System.out.println("No active brokers found.");
                return;
            }

            // Display the list of brokers to the user
            System.out.println("Active brokers:");

            for (int i = 0; i < activeBrokers.size(); i++) {
                String brokerAddress = activeBrokers.get(i);
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

            while (true) {
                System.out.println("Please select command: create, publish, show, delete.");
                String command = scanner.nextLine();
                String[] parts = command.split(" ", 3);
                if (parts.length < 1) {
                    System.out.println("Invalid command format.");
                    continue;
                }
                String topicID = null;

                String action = parts[0];
                if (parts.length > 1){
                    topicID = parts[1];
                }

                try {
                    switch (action.toLowerCase()){
                        case createTopicAction:
                            if (parts.length != 3) {
                                System.out.println("Invalid command format for create. Use: create {topic_id} {topic_name}");
                                break;
                            }
                            String topicName = parts[2];
                            String createMsg = broker.createTopic(pubName, topicName, topicID);
                            System.out.println(createMsg);
                            break;

                        case publishMessageAction:
                            if (parts.length != 3) {
                                System.out.println("Invalid command format for publish. Use: publish {topic_id} {message}");
                                break;
                            }
                            String message = parts[2];
                            String publishNotification = broker.publishMessage(pubName, topicID, message);
                            System.out.println(publishNotification);
                            break;

                        case showExistedTopicAction:
                            List<String> topicList = broker.getPubTopicList(pubName);
                            if (topicList.isEmpty()){
                                System.out.println("No topic created yet, please create one first.");
                            } else {
                                for (String topic : topicList) {
                                    System.out.println(topic);
                                }
                            }
                            break;

                        case deleteTopicAction:
                            String deleteMsg = broker.removeTopic(pubName, topicID);
                            System.out.println(deleteMsg);
                            break;

                        default:
                            System.out.println("Unknown command. Please try again.");

                    }



                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
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