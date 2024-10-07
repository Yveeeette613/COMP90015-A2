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
    private List<Topic> topicList;

    protected Broker(String brokerIP, int brokerPort, IDirectory directoryService) throws RemoteException {
        this.brokerIP = brokerIP;
        this.brokerPort = brokerPort;
        this.directoryService = directoryService;
        this.connectedPublishers = new ArrayList<>();
        this.connectedSubscribers = new ArrayList<>();
        this.messageQueue = new ConcurrentLinkedQueue<>();
        this.topicList = new ArrayList<>();
    }

//    @Override
//    public synchronized void forwardMessage(String message, Set<String> visitedBrokers) throws RemoteException {
//        if (visitedBrokers == null) {
//            visitedBrokers = new HashSet<>();
//        }
//
//        // Add current broker to the set of visited brokers
//        visitedBrokers.add(brokerIP + ":" + brokerPort);
//
//        for (String subscriber : connectedSubscribers) {
//            System.out.println("Forwarding message to subscriber: " + subscriber);
//            messageQueue.add(message);
//        }
//
//        List<String> otherBrokers = directoryService.getActiveBrokers(brokerIP, brokerPort);
//        for (String brokerAddress : otherBrokers) {
//            if (!visitedBrokers.contains(brokerAddress)) {
//                try {
//                    IBroker otherBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
//                    otherBroker.forwardMessage(message, visitedBrokers);
//                } catch (Exception e) {
//                    System.out.println("Failed to forward message to broker: " + brokerAddress);
//                    e.printStackTrace();
//                }
//            }
//        }
//    }


    public List<String> getSubscriberListForTopic(String topicID){
        for (Topic topic:topicList){
            if (topic.getTopicId().equals(topicID)){
                return topic.getSubscribersList();
            }
        }
        return null;
    }



    @Override
    public synchronized void forwardMessage(String topicID, List<String> subList, String message, Set<String> visitedBrokers) throws RemoteException {
        if (visitedBrokers == null) {
            visitedBrokers = new HashSet<>();
        }

        if (subList == null){
            for (Topic topic:topicList){
                if (topic.getTopicId().equals(topicID)){
                    subList = new ArrayList<>(topic.getSubscribersList());
                }
            }
        }


        // Add current broker to the set of visited brokers
        visitedBrokers.add(brokerIP + ":" + brokerPort);


        if (subList != null) {
            Iterator<String> iterator = subList.iterator();
            while (iterator.hasNext()) {
                String subscriber = iterator.next();
                if (connectedSubscribers.contains(subscriber)) {
                    System.out.println("Forwarding message to subscriber: " + subscriber);
                    iterator.remove();
                    messageQueue.add(message);
                }
            }

            List<String> otherBrokers = directoryService.getActiveBrokers(brokerIP, brokerPort);
            for (String brokerAddress : otherBrokers) {
                if (!visitedBrokers.contains(brokerAddress)) {
                    try {
                        IBroker otherBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
                        otherBroker.forwardMessage(topicID, subList, message, visitedBrokers);
                    } catch (Exception e) {
                        System.out.println("Failed to forward message to broker: " + brokerAddress);
                        e.printStackTrace();
                    }
                }
            }
        }
    }




    @Override
    public synchronized void forwardMessageToSub(String message, String subName, Set<String> visitedBrokers) throws RemoteException {
        if (visitedBrokers == null) {
            visitedBrokers = new HashSet<>();
        }

        // Add current broker to the set of visited brokers
        visitedBrokers.add(brokerIP + ":" + brokerPort);

        // Forward message to the specified subscriber if connected
        if (connectedSubscribers.contains(subName)) {
            System.out.println("Forwarding message to subscriber: " + subName);
            messageQueue.add(message);
        }

        // Forward message to other brokers
        List<String> otherBrokers = directoryService.getActiveBrokers(brokerIP, brokerPort);
        for (String brokerAddress : otherBrokers) {
            if (!visitedBrokers.contains(brokerAddress)) {
                try {
                    IBroker otherBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
                    otherBroker.forwardMessageToSub(message, subName, visitedBrokers);
                } catch (Exception e) {
                    System.out.println("Failed to forward message to broker: " + brokerAddress);
                    e.printStackTrace();
                }
            }
        }
    }



    @Override
    public synchronized String publishMessage(String pubName, String topicID, String message) throws RemoteException {
        String timestamp = new SimpleDateFormat("dd/MM HH:mm:ss").format(new Date());
        String topicName = null;
        String msg = null;
        for (Topic topic:topicList){
            if (topic.getTopicId().equals(topicID) && topic.getPubName().equals(pubName)){
                topicName = topic.getTopicName();
            }
        }



        if (topicName != null){
            String formattedMessage = "[" + timestamp + "] " + topicID + ":" + topicName + " " + message;
            System.out.println("Publishing message: " + formattedMessage);
            forwardMessage(topicID, null, formattedMessage, null);
            msg = "Message published: " + formattedMessage;
        } else {
            msg = "Message cannot be published because the topic was not found or the publisher does not own the topic.";
            System.out.println(msg);
        }
        return msg;

    }

//    @Override
//    public synchronized void publishMessage(String message) throws RemoteException {
//        String timestamp = new SimpleDateFormat("dd/MM HH:mm:ss").format(new Date());
//        String formattedMessage = "[" + timestamp + "] " + message;
//        System.out.println("Publishing message: " + formattedMessage);
//        forwardMessage(formattedMessage, null);
//    }



    @Override
    public String receiveMessage() throws RemoteException {
        return messageQueue.poll();
    }

    @Override
    public synchronized void notify(String message) throws RemoteException {
        System.out.println("Notification: " + message);
    }

    @Override
    public List<String> getConnectedPublishers() throws RemoteException {
        return connectedPublishers;
    }

    @Override
    public synchronized void addPublisher(String name) throws RemoteException {
        connectedPublishers.add(name);
        System.out.println("New publisher connected: " + name);
    }

    public synchronized List<String> getPubTopicList(String pubName) throws RemoteException{
        List<String> topicQueue = new ArrayList<>();

        for (Topic topic: topicList){
            if (Objects.equals(topic.getPubName(), pubName)){
                topicQueue.add("Topic ID: " + topic.getTopicId() + " Topic Name: " + topic.getTopicName() +
                        " Subscriber count: " + topic.getSubscribersList().size());

            }
        }

        return topicQueue;
    }



@Override
public synchronized List<String> getTopicList(Set<String> visitedBrokers) throws RemoteException {
    if (visitedBrokers == null) {
        visitedBrokers = new HashSet<>();
    }

    List<String> topicQueue = new ArrayList<>();

    // Add current broker to the set of visited brokers
    visitedBrokers.add(brokerIP + ":" + brokerPort);

    // Add topics from this broker
    for (Topic topic : topicList) {
        topicQueue.add("Topic ID: " + topic.getTopicId() + " Topic Name: " + topic.getTopicName() + " Publisher: " + topic.getPubName());
    }

    // Get topics from other brokers
    List<String> otherBrokers = directoryService.getActiveBrokers(brokerIP, brokerPort);
    for (String brokerAddress : otherBrokers) {
        if (!visitedBrokers.contains(brokerAddress)) {
            try {
                IBroker otherBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
                List<String> topicQueueList = otherBroker.getTopicList(visitedBrokers);
                for (String topicString : topicQueueList){
                    if (!topicQueue.contains(topicString)){
                        topicQueue.add(topicString);
                    }
                }
//                topicQueue.addAll();
            } catch (Exception e) {
                System.out.println("Failed to get topics from broker: " + brokerAddress);
                e.printStackTrace();
            }
        }
    }
    return topicQueue;
}


    public synchronized List<String> getSubscribedTopicList(String subName, Set<String> visitedBrokers) throws RemoteException {
        if (visitedBrokers == null) {
            visitedBrokers = new HashSet<>();
        }

        List<String> topicQueue = new ArrayList<>();

        // Add current broker to the set of visited brokers
        visitedBrokers.add(brokerIP + ":" + brokerPort);

        // Add topics from this broker
        for (Topic topic : topicList) {
            if (topic.getSubscribersList().contains(subName)){
                topicQueue.add("Topic ID: " + topic.getTopicId() + " Topic Name: " + topic.getTopicName() + " Publisher: " + topic.getPubName());
            }
        }

        // Get topics from other brokers
        List<String> otherBrokers = directoryService.getActiveBrokers(brokerIP, brokerPort);
        for (String brokerAddress : otherBrokers) {
            if (!visitedBrokers.contains(brokerAddress)) {
                try {
                    IBroker otherBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
//                    topicQueue.addAll(otherBroker.getSubscribedTopicList(subName, visitedBrokers));

                    List<String> topicQueueList = otherBroker.getSubscribedTopicList(subName,visitedBrokers);
                    for (String topicString : topicQueueList){
                        if (!topicQueue.contains(topicString)){
                            topicQueue.add(topicString);
                        }
                    }
//                t
                } catch (Exception e) {
                    System.out.println("Failed to get topics from broker: " + brokerAddress);
                    e.printStackTrace();
                }
            }
        }

        return topicQueue;
    }











    @Override
    public synchronized void removePublisher(String name) throws RemoteException {
        connectedPublishers.remove(name);
        System.out.println("Publisher disconnected: " + name);

        Iterator<Topic> iterator = topicList.iterator();
        while (iterator.hasNext()) {
            Topic topic = iterator.next();
            if (Objects.equals(topic.getPubName(), name)) {
                iterator.remove();
                System.out.println("Topic removed. Topic ID: " + topic.getTopicId() + ". Topic Name: " + topic.getTopicName()
                        + ". Publisher: " + topic.getPubName());
                for (String subName : topic.getSubscribersList()) {
                    String msg = "Topic removed. Topic ID: " + topic.getTopicId() + ". Topic Name: " + topic.getTopicName()
                            + ". Publisher: " + topic.getPubName();
                    forwardMessageToSub(msg, subName, null);
                }
            }
        }
    }

    @Override
    public List<String> getConnectedSubscribers() throws RemoteException {
        return connectedSubscribers;
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
        unsubscribeAllTopic(name, null);
    }

    @Override
    public synchronized String createTopic(String pubName, String topicName, String topicId) throws RemoteException {
        String msg = null;
        if (checkUnique(topicId, null)){
            Topic topic = new Topic(pubName, topicName, topicId);

            topicList.add(topic);
            msg = "New topic created. Topic ID: " + topic.getTopicId() + ". Topic Name: " + topic.getTopicName()
                    + ". Publisher: " + topic.getPubName();
            System.out.println(msg);
        } else {
            msg = "The topic ID has already been existed, please retry with a new topic ID";
            System.out.println("Topic created fail due to duplicated topic ID.");
        }
        return msg;
    }

    @Override
    public synchronized boolean checkUnique(String topicId, Set<String> visitedBroker) throws RemoteException {
        if (visitedBroker == null) {
            visitedBroker = new HashSet<>();
        }

        boolean isUnique = true;

        // Add current broker to the set of visited brokers
        visitedBroker.add(brokerIP + ":" + brokerPort);

        // Check if the topic ID exists in the current broker's topic list
        for (Topic topic : topicList) {
            if (topic.getTopicId().equals(topicId)) {
                System.out.println("Topic ID " + topicId + " is not unique.");
                return false;
            }
        }

        // Check other brokers
        List<String> otherBrokers = directoryService.getActiveBrokers(brokerIP, brokerPort);
        for (String brokerAddress : otherBrokers) {
            if (!visitedBroker.contains(brokerAddress)) {
                try {
                    IBroker otherBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
                    if (!otherBroker.checkUnique(topicId, visitedBroker)) {
                        isUnique = false;
                    }
                } catch (Exception e) {
                    System.out.println("Failed to check topic ID in broker: " + brokerAddress);
                    e.printStackTrace();
                }
            }
        }

        if (isUnique) {
            System.out.println("Topic ID " + topicId + " is unique.");
        }

        return isUnique;
    }



    @Override
    public synchronized String removeTopic(String pubName, String topicId) throws RemoteException {
        Iterator<Topic> iterator = topicList.iterator();
        String deleteMsg = "Topic cannot be removed because the topic is not existed or it isn't own by you.";
        while (iterator.hasNext()) {
            Topic topic = iterator.next();
            if (topic.getTopicId().equals(topicId) && topic.getPubName().equals(pubName)) {
                iterator.remove();
                deleteMsg = "Topic removed. Topic ID: " + topic.getTopicId() + ". Topic Name: " + topic.getTopicName()
                        + ". Publisher: " + topic.getPubName();
                System.out.println(deleteMsg);
                for (String subName : topic.getSubscribersList()) {
                    String msg = "Topic removed. Topic ID: " + topic.getTopicId() + ". Topic Name: " + topic.getTopicName()
                            + ". Publisher: " + topic.getPubName();
                    forwardMessageToSub(msg, subName, null);
                }
                break;
            }
        }
        return deleteMsg;
    }

//    @Override
//    public synchronized void subscribeTopic(String topicId, String subName) throws RemoteException {
//
//
//        for (Topic topic : topicList) {
//            if (topic.getTopicId().equals(topicId)){
//                topic.addSubscriber(subName);
//                System.out.println(subName + "subscribe topic id " + topicId);
//            }
//        }
//
//
//    }



    public synchronized void subscribeTopic(String topicId, String subName, Set<String> visitedBrokers) throws RemoteException {
        boolean topicFound = false;
        if (visitedBrokers == null) {
            visitedBrokers = new HashSet<>();
        }
        // Add current broker to the set of visited brokers
        visitedBrokers.add(brokerIP + ":" + brokerPort);

        // Subscribe to the topic in the current broker
        for (Topic topic : topicList) {
            if (topic.getTopicId().equals(topicId)) {
                topic.addSubscriber(subName);
                System.out.println(subName + " subscribed to topic id " + topicId);
                topicFound = true;
            }
        }

        // Get topics from other brokers
        List<String> otherBrokers = directoryService.getActiveBrokers(brokerIP, brokerPort);
        for (String brokerAddress : otherBrokers) {
            if (!visitedBrokers.contains(brokerAddress)) {
                try {
                    IBroker otherBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
                    otherBroker.subscribeTopic(topicId, subName, visitedBrokers);
                    topicFound = true;
                    break;
                } catch (Exception e) {
                    System.out.println("Failed to subscribe to topic from broker: " + brokerAddress);
                    e.printStackTrace();
                }
            }
        }

        if (!topicFound) {
            System.out.println("Topic ID " + topicId + " not found in any broker.");
        }
    }

    public synchronized void unsubscribeTopic(String topicId, String subName, Set<String> visitedBrokers) throws RemoteException {
        boolean topicFound = false;
        if (visitedBrokers == null) {
            visitedBrokers = new HashSet<>();
        }
        // Add current broker to the set of visited brokers
        visitedBrokers.add(brokerIP + ":" + brokerPort);

        // Subscribe to the topic in the current broker
        for (Topic topic : topicList) {
            if (topic.getTopicId().equals(topicId)) {
                topic.removeSubscriber(subName);
                System.out.println(subName + " unsubscribed to topic id " + topicId);
                topicFound = true;
            }
        }

        // Get topics from other brokers
        List<String> otherBrokers = directoryService.getActiveBrokers(brokerIP, brokerPort);
        for (String brokerAddress : otherBrokers) {
            if (!visitedBrokers.contains(brokerAddress)) {
                try {
                    IBroker otherBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
                    otherBroker.unsubscribeTopic(topicId, subName, visitedBrokers);
                    topicFound = true;
                } catch (Exception e) {
                    System.out.println("Failed to unsubscribe to topic from broker: " + brokerAddress);
                    e.printStackTrace();
                }
            }
        }

        if (!topicFound) {
            System.out.println("Topic ID " + topicId + " not found in any broker.");
        }
    }

    public synchronized void unsubscribeAllTopic(String subName, Set<String> visitedBrokers) throws RemoteException {
        if (visitedBrokers == null) {
            visitedBrokers = new HashSet<>();
        }
        // Add current broker to the set of visited brokers
        visitedBrokers.add(brokerIP + ":" + brokerPort);

        // Subscribe to the topic in the current broker
        for (Topic topic : topicList) {
            if (topic.getSubscribersList().contains(subName)) {
                topic.removeSubscriber(subName);
                System.out.println(subName + " unsubscribed to topic id " + topic.getTopicId());
            }
        }

        // Get topics from other brokers
        List<String> otherBrokers = directoryService.getActiveBrokers(brokerIP, brokerPort);
        for (String brokerAddress : otherBrokers) {
            if (!visitedBrokers.contains(brokerAddress)) {
                try {
                    IBroker otherBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
                    otherBroker.unsubscribeAllTopic(subName, visitedBrokers);
                } catch (Exception e) {
                    System.out.println("Failed to unsubscribe to topic from broker: " + brokerAddress);
                    e.printStackTrace();
                }
            }
        }

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
//            System.out.print("Enter the broker IP: ");
//            String brokerIP = scanner.nextLine();
//
//            System.out.print("Enter the broker port: ");
//            int brokerPort = Integer.parseInt(scanner.nextLine());
//
//            IDirectory directoryService = (IDirectory) Naming.lookup("//localhost:1099/DirectoryService");
//
//            System.out.println("Please enter user name, IP and port (format: username borker_ip broker_port): ");
            String brokerIP = "";
            int brokerPort = 0;
            String directoryIP = "";
            int directoryPort = 0;
            boolean validInput = false;

            while (!validInput) {
                System.out.println("Please enter the broker IP, broker port, directory service IP, and directory service port (format: brokerIP:brokerPort directoryServiceIP:directoryServicePort): ");
                String input = scanner.nextLine();
                String[] parts = input.split(" ");

                if (parts.length == 2) {
                    String[] brokerParts = parts[0].split(":");
                    String[] directoryParts = parts[1].split(":");

                    if (brokerParts.length == 2 && directoryParts.length == 2) {
                        try {
                            brokerIP = brokerParts[0];
                            brokerPort = Integer.parseInt(brokerParts[1]);
                            directoryIP = directoryParts[0];
                            directoryPort = Integer.parseInt(directoryParts[1]);
                            validInput = true;
                        } catch (NumberFormatException e) {
                            System.out.println("Invalid port number. Please enter a valid IP address and port number.");
                        }
                    } else {
                        System.out.println("Invalid format. Please enter the IP address and port number in the format: brokerIP:brokerPort directoryServiceIP:directoryServicePort");
                    }
                } else {
                    System.out.println("Invalid format. Please enter the IP address and port number in the format: brokerIP:brokerPort directoryServiceIP:directoryServicePort");
                }
            }

//            IDirectory directoryService = null;
//            try{
//                directoryService = (IDirectory) Naming.lookup("//" + directoryIP + ":" + directoryPort + "/DirectoryService");
//            } catch (Exception e) {
//                System.out.println("Cannot connect with directory. Please check the connection of directory first.");
//                e.printStackTrace();
//            }
            IDirectory directoryService = (IDirectory) Naming.lookup("//" + directoryIP + ":" + directoryPort + "/DirectoryService");
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