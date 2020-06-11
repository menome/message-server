FROM openjdk:14-alpine
COPY build/libs/messageServer-*-all.jar messageServer.jar
EXPOSE 8080
CMD ["java", "-Dcom.sun.management.jmxremote", "-Xmx512m", "-jar", "messageServer.jar"]