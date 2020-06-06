# Spring Boot MQTT Google Protobuf

This project serves two purposes:
1. Syncing data between MQTT Publisher server (Device simulator) and MQTT Subscriber server.
2. After successfully syncing, the publisher sends live data stream to the subscriber and subscriber stores the date in mysql database and also relay it to the client interface. 

[Google Protocol Buffers](https://developers.google.com/protocol-buffers/docs/javatutorial) are used for data communication between the MQTT publisher and the subscriber.

##### To compile Protocol Buffers using Maven, please refer [this article](https://dzone.com/articles/compile-protocol-buffers-using-maven)

_For further documentation regarding project, please refer Documents folder._
