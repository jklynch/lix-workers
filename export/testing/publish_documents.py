import argparse

from bluesky_kafka import Publisher
import databroker


def publish_documents(scan_id, topic, bootstrap_servers):
    kafka_publisher = Publisher(
        topic=topic,
        bootstrap_servers=bootstrap_servers,
        key="lix.export.worker.testing",
        producer_config={
            "acks": 1,
            "request.timeout.ms": 5000,
        }
    )

    db = databroker.Broker.named("lix")
    for name, doc in db[scan_id].documents():
        print(f"publishing document {name}")
        kafka_publisher(name, doc)


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--scan-id", type=str)
    argparser.add_argument("--topic", type=str, default="lix.bluesky.documents")
    argparser.add_argument("--bootstrap-servers", type=str, help="comma-delimited list", default="10.0.137.8:9092")

    args = argparser.parse_args()
    print(args)

    publish_documents(**vars(args))
