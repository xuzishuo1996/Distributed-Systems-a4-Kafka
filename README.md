# Distributed Systems - Assignment 4 - Kafka

Implemented a simple real-time stream processing application using the **Kafka Streams API**.

### Inputs

Consumes records from two Kafka topics:

1) The first topic provides info on students and their whereabouts.

2) The second topic provides info on classrooms and their capacity.

### Outputs

Outputs (commits to the output topic) the names of rooms for which the current occupancy exceeds the maximum capacity, along with some additional information.

### Properties

1) Each input message is consumed and processed in around one second or less.

2) Tolerates crash failures.

