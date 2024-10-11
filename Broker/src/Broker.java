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


    //Send message to all subscribers
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
                    if (!messageQueue.contains(message)){
                        messageQueue.add(message);
                    }
                }
            }

            List<String> otherBrokers = directoryService.getActiveBrokers(brokerIP, brokerPort);
            for (String brokerAddress : otherBrokers) {
                if (!visitedBrokers.contains(brokerAddress)) {
                    try {
                        IBroker otherBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
                        otherBroker.forwardMessage(topicID, subList, message, visitedBrokers);
                        break;
                    } catch (Exception e) {
                        System.out.println("Failed to forward message to broker: " + brokerAddress);
                        e.printStackTrace();
                    }
                }
            }
        }
    }


    //send message to corresponding subscriber
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
                break;
            }
        }
    }


    //Publish Message by publisher who created this topic
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
            String formattedMessage = timestamp + " " + topicID + ":" + topicName + " " + message;
            System.out.println("Publishing message: " + formattedMessage);
            forwardMessage(topicID, null, formattedMessage, null);
            msg = "Message published: " + formattedMessage;
        } else {
            msg = "Message cannot be published because the topic was not found or the publisher does not own the topic.";
            System.out.println(msg);
        }
        return msg;

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


    //Get all topic list for subscriber
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
                    break;

                } catch (Exception e) {
                    System.out.println("Failed to get topics from broker: " + brokerAddress);
                    e.printStackTrace();
                }
            }
        }
        return topicQueue;
    }

    //Get subscribed topic list
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
                    List<String> topicQueueList = otherBroker.getSubscribedTopicList(subName,visitedBrokers);
                    for (String topicString : topicQueueList){
                        if (!topicQueue.contains(topicString)){
                            topicQueue.add(topicString);
                        }
                    }
                    break;
                } catch (Exception e) {
                    System.out.println("Failed to get topics from broker: " + brokerAddress);
                    e.printStackTrace();
                }
            }
        }

        return topicQueue;
    }

    //Remove publisher when they crash
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

    // Create topic
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

    //check if the topic have already existed
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
                    break;

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


    //Remove topic
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

    //Subscribe topic
    public synchronized Boolean subscribeTopic(Boolean topicFound, String topicId, String subName, Set<String> visitedBrokers) throws RemoteException {

        if (visitedBrokers == null) {
            visitedBrokers = new HashSet<>();
        }
        // Add current broker to the set of visited brokers
        visitedBrokers.add(brokerIP + ":" + brokerPort);

        // Subscribe to the topic in the current broker
        for (Topic topic : topicList) {
            if (topic.getTopicId().equals(topicId) && !topic.getSubscribersList().contains(subName)) {
                topic.addSubscriber(subName);
                System.out.println(subName + " subscribed to topic id " + topicId);
                topicFound = true;
            }
        }

        if (!topicFound) {
            // Get topics from other brokers
            List<String> otherBrokers = directoryService.getActiveBrokers(brokerIP, brokerPort);
            for (String brokerAddress : otherBrokers) {
                if (!visitedBrokers.contains(brokerAddress)) {
                    try {
                        IBroker otherBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
                        topicFound = otherBroker.subscribeTopic(topicFound,topicId, subName, visitedBrokers);

                        break;
                    } catch (Exception e) {
                        System.out.println("Failed to subscribe to topic from broker: " + brokerAddress);
                        e.printStackTrace();
                    }
                }
            }
        }

        return topicFound;
    }


    //Unsubscribe topic
    public synchronized Boolean unsubscribeTopic(Boolean topicFound, String topicId, String subName, Set<String> visitedBrokers) throws RemoteException {
        if (visitedBrokers == null) {
            visitedBrokers = new HashSet<>();
        }
        // Add current broker to the set of visited brokers
        visitedBrokers.add(brokerIP + ":" + brokerPort);

        // Unsubscribe from the topic in the current broker
        for (Topic topic : topicList) {
            if (topic.getTopicId().equals(topicId) && topic.getSubscribersList().contains(subName)) {
                topic.removeSubscriber(subName);
                System.out.println(subName + " unsubscribed to topic id " + topicId);
                topicFound = true;
                return topicFound;
            }
        }
        // Get topics from other brokers
        List<String> otherBrokers = directoryService.getActiveBrokers(brokerIP, brokerPort);
        for (String brokerAddress : otherBrokers) {
            if (!visitedBrokers.contains(brokerAddress)) {
                try {
                    IBroker otherBroker = (IBroker) Naming.lookup("//" + brokerAddress + "/Broker");
                    topicFound = otherBroker.unsubscribeTopic(topicFound, topicId, subName, visitedBrokers);
                    if (topicFound){
                        return topicFound;
                    }
                    break;
                } catch (Exception e) {
                    System.out.println("Failed to unsubscribe to topic from broker: " + brokerAddress);
                    e.printStackTrace();
                }
            }
        }

        return topicFound;
    }

    //Unsubscribe All topic when a subscriber crash
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
                    break;
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
            String brokerIP = "";
            int brokerPort = 0;
            String directoryIP = "";
            int directoryPort = 0;
            Broker broker = null;

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

                            IDirectory directoryService = (IDirectory) Naming.lookup("//" + directoryIP + ":" + directoryPort + "/DirectoryService");
                            broker = new Broker(brokerIP, brokerPort, directoryService);
                            java.rmi.registry.LocateRegistry.createRegistry(brokerPort);
                            Naming.rebind("//" + brokerIP + ":" + brokerPort + "/Broker", broker);
                            System.out.println("Broker is running on " + brokerIP + ":" + brokerPort);

                            broker.registerAndConnect();


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


            Broker finalBroker = broker;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    finalBroker.disconnect();
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            }));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}