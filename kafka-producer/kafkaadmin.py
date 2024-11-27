from kafka.admin import KafkaAdminClient, NewTopic


def delete_topics(admin):
    admin.delete_topics(topics=["transaction"])
    admin.delete_topics(topics=["store_transaction_count"])


def create_topics(admin, topic_list):
    admin.create_topics(new_topics=topic_list, validate_only=False)


if __name__ == "__main__":
    admin_client = KafkaAdminClient(
        bootstrap_servers="35.223.205.191:9092", client_id="DE2"
    )  # use your VM's external IP Here!
    topic_list = [
        NewTopic(name="transaction", num_partitions=1, replication_factor=1),
        NewTopic(
            name="store_transaction_count", num_partitions=1, replication_factor=1
        ),
    ]
    delete_topics(admin_client)
    create_topics(admin_client, topic_list)
