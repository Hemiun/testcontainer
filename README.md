## Test containers
Package that makes it simple to create and clean up container-based dependencies for automated integration/smoke tests.
For implementation used library [testcontainers-go]("https://github.com/testcontainers/testcontainers-go").  
Implemented containers for postgreSQL, apache zookepeer, apache kafka
Postgresql
- Starting and stopping a container
- Database preparation (database creation, users creation, schema creation)
- applying all up migrations

Kafka
- Starting and stopping a containers (zookeper + kafka). ZooKeeper and broker started in the common docker network. A new network is created for each instance of the KafkaContainer. This avoids concurrent tests.
- Custom strategy for kafka broker readiness check (through metadata acquisition) implemented
