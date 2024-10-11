//Student Name: Quan Yi
//Student ID: 1054540

import Interface.IBroker;
import Interface.IDirectory;
import java.rmi.Naming;
import java.util.List;
import java.util.Scanner;

public class Subscriber {
    private static String subName;
    private static IBroker broker = null;
    private static final String listAllTopicAction = "list";
    private static final String subscribeTopicAction = "sub";
    private static final String currentSubscriptionTopicAction = "current";
    private static final String unsubscribeTopicAction = "unsub";


    public static void main(String[] args) {
        try {
            Scanner scanner = new Scanner(System.in);
            String directoryIP = "";
            int directoryPort = 0;
            boolean validInput = false;
            List<String> activeBrokers = null;

            while (!validInput) {
                System.out.println("Please enter username directory_ip directory_port: ");
                String input = scanner.nextLine();
                String[] parts = input.split(" ");

                if (parts.length == 3) {
                    try {
                        subName = parts[0];
                        directoryIP = parts[1];
                        directoryPort = Integer.parseInt(parts[2]);
                        validInput = true;
                        IDirectory directoryService = (IDirectory) Naming.lookup("//" + directoryIP + ":" + directoryPort + "/DirectoryService");
                        activeBrokers = directoryService.getActiveBrokers("", 0);
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid port number. Please enter a valid username, IP address, and port number.");
                    }
                } else {
                    System.out.println("Invalid format. Please enter the username, IP address, and port number in the format: username directory_ip directory_port");
                }
            }

//            IDirectory directoryService = (IDirectory) Naming.lookup("//" + directoryIP + ":" + directoryPort + "/DirectoryService");
//            List<String> activeBrokers = directoryService.getActiveBrokers("", 0);

            if (activeBrokers.isEmpty()) {
                System.out.println("No active brokers found.");
                return;
            }

            System.out.println("Active brokers:");
            for (int i = 0; i < activeBrokers.size(); i++) {
                String brokerAddress = activeBrokers.get(i);
                System.out.println((i + 1) + ". " + brokerAddress);
            }

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

            broker = (IBroker) Naming.lookup("//" + brokerIP + ":" + brokerPort + "/Broker");

            System.out.println("Connected to broker " + brokerIP + ":" + brokerPort);
            broker.addSubscriber(subName);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (broker != null) {
                    try {
                        broker.removeSubscriber(subName);
                        System.out.println("Disconnected from broker.");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }));

            // Start the message receiver thread
            Thread messageReceiverThread = new Thread(new MessageReceiver(broker));
            messageReceiverThread.start();

            // Main thread for other functions
            while (true) {
                System.out.println("Please select command: list, sub, current, unsub.");
                String command = scanner.nextLine();
                String[] parts = command.split(" ");
                if (parts.length > 2) {
                    System.out.println("Invalid command format.");
                    continue;
                }
                String topicID = null;

                String action = parts[0];
                if (parts.length > 1){
                    topicID = parts[1];
                }

                try {
                    switch (action.toLowerCase()) {

                        case listAllTopicAction:
                            List<String> topicList = broker.getTopicList(null);
                            if (topicList.isEmpty()){
                                System.out.println("No topic created now, please check later.");
                            } else {
                                for (String topic : topicList) {
                                    System.out.println(topic);
                                }
                            }
                            break;

                        case subscribeTopicAction:
                            if (parts.length != 2) {
                                System.out.println("Invalid command format for subscribe. Use: sub {topic_id}");
                                break;
                            }
                            Boolean topicSubscribed = broker.subscribeTopic(false, topicID, subName, null);
                            if (topicSubscribed){
                                System.out.println("Subscribed to topic " + topicID);
                            } else {
                                System.out.println("Cannot subscribe to the topic because the topic isn't existed or you have already subscribed it.");
                            }
                            break;

                        case currentSubscriptionTopicAction:
                            List<String> subscribedTopicList = broker.getSubscribedTopicList(subName, null);
                            if (subscribedTopicList.isEmpty()){
                                System.out.println("No topic subscribed, please subscribe first.");
                            } else {
                                for (String topic : subscribedTopicList) {
                                    System.out.println(topic);
                                }
                            }
                            break;

                        case unsubscribeTopicAction:
                            if (parts.length != 2) {
                                System.out.println("Invalid command format for unsubscribe. Use: unsub {topic_id}");
                                break;
                            }
                            Boolean topicUnsubscribed = broker.unsubscribeTopic(false, topicID, subName, null);
                            if (topicUnsubscribed){
                                System.out.println("Unsubscribed to topic " + topicID);
                            } else {
                                System.out.println("Cannot unsubscribe to the topic because the topic isn't existed or you didn't subscribe it.");
                            }
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
            System.exit(1);
        } finally {
            if (broker != null) {
                try {
                    broker.removeSubscriber(subName);
                    System.out.println("Disconnected from broker.");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}