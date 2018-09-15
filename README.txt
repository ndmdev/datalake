Create a directory to store the parquet files and configure it in application.yml file
eg:-
      outputBaseLocation: /tmp/stream/1

Starting Stream Sink (Twitter Implementation)
-----------------------------
java -jar stream-sink-0.0.1-SNAPSHOT.jar -Dspring.cloud.stream.default-binder=kafka -Dspring.cloud.stream.bindings.input.destination=test-1 -Dspring.cloud.stream.kafka.binder.zkNodes=localhost:2181 -Dspring.config.location=application.yml

Starting Stream Source (Twitter)
--------------------------------
java -jar twitterstream-source-kafka-10-1.2.0.RELEASE.jar -Dspring.cloud.stream.kafka.binder.zkNodes=localhost -Dspring.cloud.stream.bindings.output.destination=test-1 --twitter.credentials.access-token=922336556391604224-VOAh2csgf2GtEJO9uRCOqLVqJlUiuyd --twitter.credentials.access-token-secret=WucVG15PL1oKayb6628D0axdyhX60SHwOJLqqcuEGnMah --twitter.credentials.consumer-key=USmzLoKOQGJXthOQcyBF8i4So --twitter.credentials.consumer-secret=9fHZB0eccEiZzR0Mz03qumOw3eSoAmyVwC7dOpa2ebV6YwKOQL


