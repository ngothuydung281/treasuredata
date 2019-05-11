# Install and Setup
- Java version 1.8 or higher: https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html
- Maven https://maven.apache.org/download.cgi
- Set JAVA_HOME Environment variable and add %JAVA_HOME%\bin to Path environment variable
- Set MAVEN_HOME Environment variable and add %JAVA_HOME%\bin to Path environment variable

#Usage
- Open cmd, cd to local porject folder
- mvn clean install
- mvn compile
- run command line. Example: mvn exec:java -q -Dexec.args="-f tabular -e presto -c 'name' -m 1557236480 -M 1557236516 -l 1 mytest_database table_customer"
