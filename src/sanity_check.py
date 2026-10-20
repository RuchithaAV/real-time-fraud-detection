"""
Sanity check script to verify Python connectivity to:
1. Redis (State Store)
2. PostgreSQL (Database)
3. Apache Kafka (Event Streaming)
"""

import json
import sys
import time

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import redis
import psycopg2
from confluent_kafka import Producer, Consumer, KafkaError


def test_redis() -> bool:
    print("\n[1/3] Testing Redis connection...")
    try:
        r = redis.Redis(host="localhost", port=6379, db=0, socket_timeout=5)
        r.set("sanity_test_key", "pong")
        val = r.get("sanity_test_key")
        r.delete("sanity_test_key")
        if val == b"pong":
            print("  [OK] Redis is healthy and responding (PONG received).")
            return True
        else:
            print(f"  [FAIL] Unexpected Redis value: {val}")
            return False
    except Exception as e:
        print(f"  [FAIL] Redis connection failed: {e}")
        return False


def test_postgres() -> bool:
    print("\n[2/3] Testing PostgreSQL connection...")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            dbname="fraud_detection",
            user="fraud_admin",
            password="fraud_secure_password",
            connect_timeout=5,
        )
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"  [OK] PostgreSQL is healthy.")
        print(f"    Version: {version[:45]}...")
        return True
    except Exception as e:
        print(f"  [FAIL] PostgreSQL connection failed: {e}")
        return False


def test_kafka() -> bool:
    print("\n[3/3] Testing Apache Kafka (Producer & Consumer)...")
    topic = "test-sanity-topic"
    test_payload = {"status": "success", "message": "hello kafka", "timestamp": time.time()}

    try:
        # 1. Producer test
        conf_producer = {"bootstrap.servers": "127.0.0.1:9092"}
        producer = Producer(conf_producer)
        print("  - Producing test message to topic:", topic)
        producer.produce(topic, value=json.dumps(test_payload).encode("utf-8"))
        producer.flush(timeout=10)
        print("  [OK] Message produced and flushed successfully.")

        # 2. Consumer test
        print("  - Consuming test message from topic...")
        conf_consumer = {
            "bootstrap.servers": "127.0.0.1:9092",
            "group.id": f"sanity-group-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
        consumer = Consumer(conf_consumer)
        consumer.subscribe([topic])

        received = False
        start_wait = time.time()
        while time.time() - start_wait < 10:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"  [FAIL] Consumer error: {msg.error()}")
                    break
            data = json.loads(msg.value().decode("utf-8"))
            if data.get("status") == "success":
                print(f"  [OK] Consumed message correctly: {data}")
                received = True
                break

        consumer.close()
        if not received:
            print("  [FAIL] Consumer timed out without receiving the test message.")
            return False

        print("  [OK] Kafka Producer & Consumer are healthy.")
        return True
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"  [FAIL] Kafka test failed: {e}")
        return False


def main():
    print("=" * 60)
    print("REAL-TIME FRAUD DETECTION: INFRASTRUCTURE SANITY CHECK")
    print("=" * 60)

    redis_ok = test_redis()
    postgres_ok = test_postgres()
    kafka_ok = test_kafka()

    print("\n" + "=" * 60)
    if redis_ok and postgres_ok and kafka_ok:
        print("[SUCCESS] ALL 3 SERVICES ARE HEALTHY AND ACCESSIBLE FROM PYTHON!")
        print("=" * 60)
    else:
        print("[WARNING] ONE OR MORE SERVICES FAILED. Review errors above.")
        print("=" * 60)


if __name__ == "__main__":
    main()
