# Bridge Exporter (Bridge-EX)
Worker app to get all uploads for each study and generate aggregation reports every day and every week

Set-up:

To run a full build (including compile, unit tests, findbugs, and jacoco test coverage), run:
mvn verify

(A full build takes about 1 min 15 seconds on my laptop, from a clean workspace.)

To just run findbugs, run:
mvn compile findbugs:check

To run findbugs and get a friendly GUI to read about the bugs, run:
mvn compile findbugs:findbugs findbugs:gui

To run jacoco coverage reports and checks, run:
mvn test jacoco:report jacoco:check

Jacoco report will be in target/site/jacoco/index.html

To run this locally, run
mvn spring-boot:run


Useful Spring Boot / Maven development resouces:
http://stackoverflow.com/questions/27323104/spring-boot-and-maven-exec-plugin-issue
http://techblog.molindo.at/2007/11/maven-unable-to-find-resources-in-test-cases.html
