# apache-avro-demistified
A project to show how to implement Apache Avro Java APIs

Compiling and running the project
$ mvn compile

Serialize and Deserialze with code generation 
$ mvn -q exec:java -Dexec.mainClass=com.ksr.SpecificMain -Dorg.apache.avro.specific.use_custom_coders=true

Serialize and Deserialze without code generation 
$ mvn -q exec:java -Dexec.mainClass=com.ksr.GenericMain -Dorg.apache.avro.specific.use_custom_coders=true

