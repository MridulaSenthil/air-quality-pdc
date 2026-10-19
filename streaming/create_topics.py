from confluent_kafka.admin import AdminClient, NewTopic

admin = AdminClient({"bootstrap.servers": "localhost:9092"})
topics = [
    NewTopic("aq.readings", num_partitions=4, replication_factor=1),
    NewTopic("aq.processed", num_partitions=4, replication_factor=1),
    NewTopic("aq.alerts", num_partitions=1, replication_factor=1),
]
for name, fut in admin.create_topics(topics).items():
    try:
        fut.result()
        print(f"created {name}")
    except Exception as e:
        print(f"{name}: {e}")