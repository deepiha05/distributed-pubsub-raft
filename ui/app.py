# ui/app.py
import json
import os

import requests

# Any broker works; followers forward to the leader. If one broker is down, try the next.
BROKER_URLS = [u.strip() for u in os.environ.get(
    "BROKER_URLS", "http://localhost:8000,http://localhost:8010,http://localhost:8020").split(",") if u.strip()]


def call(method, path, **kwargs):
    last_error = None
    for url in BROKER_URLS:
        try:
            response = requests.request(method, url + path, timeout=15, **kwargs)
        except requests.RequestException as e:
            last_error = e
            continue
        try:
            print(json.dumps(response.json(), indent=2))
        except ValueError:
            print(response.status_code, response.text)
        return
    print(f"No broker reachable: {last_error}")


def create_topic():
    name = input("Topic name: ")
    partitions = int(input("Number of partitions: "))
    retention = int(input("Retention duration in seconds (0 = keep forever): "))
    call("POST", "/topics", json={"name": name, "partitions": partitions, "retention_seconds": retention})


def publish_message():
    topic = input("Topic name: ")
    payload = input("Message payload: ")
    key = input("Message key (optional, press Enter to skip): ")
    call("POST", f"/topics/{topic}/messages", json={"payload": payload, "key": key or None})


def list_topics():
    call("GET", "/topics")


def update_retention():
    topic = input("Topic name: ")
    retention = int(input("New retention in seconds (0 = keep forever): "))
    call("PUT", f"/topics/{topic}/retention", json={"retention_seconds": retention})


def create_subscription():
    subscriber_id = input("Subscriber ID: ")
    topic = input("Topic to subscribe: ")
    data = {"subscriber_id": subscriber_id, "topic": topic}
    dl_topic = input("Dead-letter topic (optional, press Enter to skip): ")
    if dl_topic:
        data["dead_letter_topic"] = dl_topic
        data["max_delivery_attempts"] = int(input("Max delivery attempts before dead-lettering: ") or 5)
    call("POST", "/subscriptions", json=data)


def set_dead_letter_policy():
    subscription_id = input("Subscription ID: ")
    dl_topic = input("Dead-letter topic (Enter to remove the policy): ")
    data = {"dead_letter_topic": dl_topic or None}
    if dl_topic:
        data["max_delivery_attempts"] = int(input("Max delivery attempts before dead-lettering: ") or 5)
    call("PUT", f"/subscriptions/{subscription_id}/dead-letter", json=data)


def join_group():
    subscription_id = input("Subscription ID (consumer group): ")
    consumer_id = input("Consumer ID: ")
    call("POST", f"/subscriptions/{subscription_id}/members", json={"consumer_id": consumer_id})


def leave_group():
    subscription_id = input("Subscription ID (consumer group): ")
    consumer_id = input("Consumer ID: ")
    call("DELETE", f"/subscriptions/{subscription_id}/members/{consumer_id}")


def pull_messages():
    subscription_id = input("Subscription ID: ")
    consumer_id = input("Consumer ID (only for consumer groups, Enter to skip): ")
    params = {"consumer_id": consumer_id} if consumer_id else {}
    call("GET", f"/subscriptions/{subscription_id}/messages", params=params)


def ack_messages():
    subscription_id = input("Subscription ID: ")
    partition = int(input("Partition (integer): "))
    offset = int(input("Offset to ack (last message offset received): "))
    call("POST", f"/subscriptions/{subscription_id}/ack",
         json={"acks": [{"partition": partition, "offset": offset}]})


def seek():
    subscription_id = input("Subscription ID: ")
    partition = int(input("Partition (integer): "))
    offset = int(input("Offset to replay from: "))
    call("POST", f"/subscriptions/{subscription_id}/seek", json={"partition": partition, "offset": offset})


def delete_topic():
    topic = input("Enter the topic name to delete: ")
    call("DELETE", f"/topics/{topic}")


def delete_all_topics():
    confirmation = input("Are you sure you want to delete ALL topics? (yes/no): ")
    if confirmation.lower() == "yes":
        call("DELETE", "/topics")
    else:
        print("Operation cancelled.")


def cluster_status():
    for url in BROKER_URLS:
        try:
            s = requests.get(url + "/health", timeout=2).json()
            print(f"{url}: {s['id']} {s['role']} (term {s['term']}, leader {s['leader']})")
        except requests.RequestException:
            print(f"{url}: DOWN")


MENU = [
    ("Create Topic", create_topic),
    ("Publish Message", publish_message),
    ("List Topics", list_topics),
    ("Create Subscription", create_subscription),
    ("Pull Messages", pull_messages),
    ("Acknowledge Messages", ack_messages),
    ("Delete a Topic", delete_topic),
    ("Delete All Topics", delete_all_topics),
    ("Update Retention", update_retention),
    ("Join Consumer Group", join_group),
    ("Leave Consumer Group", leave_group),
    ("Replay From Offset (seek)", seek),
    ("Set Dead-Letter Policy", set_dead_letter_policy),
    ("Cluster Status", cluster_status),
]


def main():
    while True:
        print("\n--- Messaging System UI ---")
        for i, (label, _) in enumerate(MENU, 1):
            print(f"{i}. {label}")
        print("0. Exit")
        try:
            choice = input("Enter choice: ")
        except EOFError:
            print("No input received. Exiting UI.")
            break
        if choice == "0":
            break
        if choice.isdigit() and 1 <= int(choice) <= len(MENU):
            try:
                MENU[int(choice) - 1][1]()
            except ValueError:
                print("Invalid number")
        else:
            print("Invalid choice")


if __name__ == "__main__":
    main()
