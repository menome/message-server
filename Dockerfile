FROM openjdk:17
COPY build/libs/messageServer-*-all.jar messageServer.jar
EXPOSE 8081
CMD ["java", "-Dcom.sun.management.jmxremote", "-Xmx512m", "-jar", "messageServer.jar"]