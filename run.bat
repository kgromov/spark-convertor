set JAVA_HOME=%JAVA_11%
mvn spring-boot:run -Dspring-boot.run.arguments="--spring.profiles.active=kafka  --server.port=8082"