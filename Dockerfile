FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 messageserver
COPY build/libs/messageServer-*-all.jar /app/messageServer.jar
USER messageserver
EXPOSE 8081
CMD ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/messageServer.jar"]
