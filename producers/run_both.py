"""Runs telemetry_producer and daily_cost_producer concurrently in one container."""
import multiprocessing

import daily_cost_producer
import telemetry_producer

if __name__ == "__main__":
    p1 = multiprocessing.Process(target=telemetry_producer.main)
    p2 = multiprocessing.Process(target=daily_cost_producer.main)
    p1.start()
    p2.start()
    p1.join()
    p2.join()
