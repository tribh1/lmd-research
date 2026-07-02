# Hive Metastore with the JDBC and S3 jars the stock apache/hive image lacks:
# - postgresql: HMS backend database driver (DB_DRIVER=postgres)
# - hadoop-aws + aws-sdk-bundle: create/validate warehouse dirs on MinIO (s3a://)
FROM apache/hive:3.1.3

USER root
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar /opt/hive/lib/postgresql.jar
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar /opt/hive/lib/hadoop-aws.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar /opt/hive/lib/aws-sdk-bundle.jar
RUN chmod 644 /opt/hive/lib/postgresql.jar /opt/hive/lib/hadoop-aws.jar /opt/hive/lib/aws-sdk-bundle.jar
USER hive
