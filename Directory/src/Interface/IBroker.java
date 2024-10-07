package Interface;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Set;

public interface IBroker extends Remote {
    // Forward message to other brokers
    void forwardMessage(String topicID, List<String> subList, String message, Set<String> visitedBrokers) throws RemoteException;

//    void forwardMessage(String message, Set<String> visitedBrokers) throws RemoteException;


    // Notify the broker of an event
    void notify(String message) throws RemoteException;

    //Publish a Message
    String publishMessage(String pubName, String topicID, String message) throws RemoteException;
//    void publishMessage(String message) throws RemoteException;


    //Receive a Message
    String receiveMessage() throws RemoteException;

    // Check the number of connected publishers
    List<String> getConnectedPublishers() throws RemoteException;

    // Add a publisher
    void addPublisher(String name) throws RemoteException;

    // Remove a publisher
    void removePublisher(String name) throws RemoteException;

    // Check the number of connected Subscribers
    List<String> getConnectedSubscribers() throws RemoteException;

    // Add a Subscriber
    void addSubscriber(String name) throws RemoteException;

    // Remove a Subscriber
    void removeSubscriber(String name) throws RemoteException;

    //Create a Topic
    String createTopic(String pubName, String topicName, String topicId) throws RemoteException;

    //Remove a Topic
    String removeTopic(String pubName, String topicId) throws RemoteException;

    //Subscribe a Topic
    void subscribeTopic(String topicId, String subName, Set<String> visitedBrokers) throws RemoteException;

    //Unsubscribe a Topic
    void unsubscribeTopic(String topicId, String subName, Set<String> visitedBrokers) throws RemoteException;

    //Unsubscribe all Topic
    void unsubscribeAllTopic(String subName, Set<String> visitedBrokers) throws RemoteException;

    //Return the list of Subscribed Topic
    List<String> getSubscribedTopicList(String subName, Set<String> visitedBrokers) throws RemoteException;

    //Return the Topic list
    List<String> getTopicList(Set<String> visitedBrokers) throws RemoteException;

    //Return the Topic list for the publisher
    List<String> getPubTopicList(String pubName) throws RemoteException;

    void forwardMessageToSub(String message, String subName, Set<String> visitedBrokers) throws RemoteException;

    boolean checkUnique(String topicId, Set<String> visitedBroker) throws RemoteException;
}

