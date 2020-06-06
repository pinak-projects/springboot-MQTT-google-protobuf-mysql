# Spring Boot MQTT Google Protobuf

Project brief:
1. Syncing data between MQTT Publisher server (AMS Device simulator) and MQTT Subscriber server.
2. After successful syncing, the publisher sends live data stream to the subscriber and subscriber stores the data in mysql database and also relay it to the client interface. 

![Screenshot](https://github.com/pinakjakhr/springboot-MQTT-google-protobuf-mysql/blob/master/0.%20Documents/MQTT.jpg)

[Google Protocol Buffers](https://developers.google.com/protocol-buffers/docs/javatutorial) are used for data communication between MQTT publisher and MQTT subscriber.

VerneMQ is used as MQTT broker, please refer [installation doc](https://docs.vernemq.com/installation/debian_and_ubuntu)

To compile Protocol Buffers using Maven, please refer [this article](https://dzone.com/articles/compile-protocol-buffers-using-maven)

##### _For further documentation regarding project, please refer Documents folder._
