import argparse


def main():
    parser = argparse.ArgumentParser(description="Mekong ingestion pipeline")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    sub.add_parser("smoke-producer",        help="Send one hardcoded message to Kafka")
    sub.add_parser("smoke-consumer",        help="Print every message on stock.price.realtime")
    sub.add_parser("stock-price-producer",  help="Poll vnstock price board → Kafka (every 30 s)")
    sub.add_parser("crypto-price-producer", help="Poll crypto exchange prices → Kafka (every 5 s)")
    sub.add_parser("storage-consumer",      help="Consume all topics and write to MinIO as Avro")

    args = parser.parse_args()

    if args.command == "smoke-producer":
        import json
        from market_data_models.message import build_envelope
        from model.base_producer import BaseProducer
        from market_data_models.topics import STOCK_PRICE_REALTIME

        msg = build_envelope(
            event_type="price.snapshot",
            symbol="VCB",
            exchange="HOSE",
            payload={
                "price": 85000,
                "change": 500,
                "pct_change": 0.59,
                "volume": 1_234_567,
                "bid": 84900,
                "ask": 85100,
            },
        )
        with BaseProducer(topic=STOCK_PRICE_REALTIME) as p:
            p.send(STOCK_PRICE_REALTIME, value=msg, key="VCB")
            p.flush()
        print(f"Sent to {STOCK_PRICE_REALTIME}:\n{json.dumps(msg, indent=2)}")

    elif args.command == "smoke-consumer":
        import json
        from model.base_consumer import BaseConsumer
        from market_data_models.topics import STOCK_PRICE_REALTIME

        print("Listening on stock.price.realtime — press Ctrl+C to stop.\n")
        with BaseConsumer([STOCK_PRICE_REALTIME], group_id="smoke") as c:
            for msg in c.messages():
                print(
                    f"partition={msg.partition}  offset={msg.offset}  key={msg.key}\n"
                    f"{json.dumps(msg.value, indent=2)}\n"
                )

    elif args.command == "stock-price-producer":
        from producers.stock_price_producer import run
        run()

    elif args.command == "crypto-price-producer":
        from producers.crypto_price_producer import run
        run()

    elif args.command == "storage-consumer":
        from consumers.storage_consumer import run
        run()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
