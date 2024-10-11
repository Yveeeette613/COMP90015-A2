# Introduction

This project is a publisher-subscriber (pub-sub) system designed to offer real-time communication with high efficiency and scalability using **Remote Method Invocation (RMI)**. The system architecture involves **publishers** and **subscribers** connecting to **brokers** to communicate with each other. A **directory** service is used to facilitate the connection between brokers, publishers, and subscribers.

Once a connection is established, publishers and subscribers can select the broker they want to connect to via the directory service. Each broker will only store information about the publishers and subscribers connected to it.

### Key Features:
- **Publishers** can create their own topics and publish messages related to those topics. They are only allowed to publish or delete the topics they created.
- **Subscribers** can subscribe to topics from any publisher within the interconnected brokers and receive real-time messages from those topics.
- The system doesn’t store user information permanently, so when a publisher or subscriber disconnects, the related data (e.g., topic, subscription) is automatically deleted.

### Command Format:
Due to the implementation of the bonus part and the use of RMI, the command arguments differ slightly from the specification. To run the system, use the following command format:

#### Directory:
```bash
java -jar Directory.jar <directory_ip> <directory_port>
```

#### Broker:
```bash
java -jar Broker.jar <broker_ip:broker_port> <directory_ip:directory_port>
```

#### Publisher:
```bash
java -jar Publisher.jar <username> <directory_ip> <directory_port>
```

#### Subscriber:
```bash
java -jar Subscriber.jar <username> <directory_ip> <directory_port>
```


# Assumptions

- **Topic name**, **publisher name**, and **subscriber name** are always a single word.
- **Publisher names** and **subscriber names** are always unique.
- The format of **topic ID** is a string.
- Three interconnected brokers will remain operational and will not crash.
- All brokers, publishers, and subscribers know the **IP address** and **port number** of the directory.
- The underlying network is reliable, and messages will not be lost.
