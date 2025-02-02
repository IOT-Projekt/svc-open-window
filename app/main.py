from kafka_handler import KafkaConfig, setup_kafka_consumer
import threading
import json
import logging
import os

# Set up basic logging
logging.basicConfig(level=logging.INFO)

OPEN_WINDOW_STR = "Öffne die Fenster, um die Luftfeuchtigkeit zu vebessern."
CLOSE_WINDOW_STR = (
    "Bitte lasse die Fenster geschlossen, um die Luftfeuchtigkeit zu erhalten."
)

# Kafka topics from environment variables
INDOOR_TOPIC = os.getenv("KAFKA_INDOOR_TOPIC", "humidity")
OUTDOOR_TOPIC = os.getenv("KAFKA_OUTDOOR_TOPIC", "open_weather")


def should_open_windows(indoor_humidity, outdoor_humidity) -> str:
    """
    This function determines if you should open the windows based on the indoor and outdoor humidity.
    If the indoor humidity is below 40%, it recommends opening the windows. Between 40% and 60%, it always recommends closing the windows.
    If the indoor humidity is above 60% and the outdoor humidity is lower than the indoor humidity, it recommends opening the windows.
    """
    if indoor_humidity < 40:
        return OPEN_WINDOW_STR
    elif indoor_humidity > 40 and indoor_humidity < 60:
        return CLOSE_WINDOW_STR
    elif indoor_humidity > 60 and outdoor_humidity < indoor_humidity:
        return OPEN_WINDOW_STR

    return CLOSE_WINDOW_STR


def consume_indoor_humidity_messages(indoor_consumer, shared_data, lock) -> None:
    for indoor_msg in indoor_consumer:
        # Get humidity and timestamp from message
        indoor_humidity = indoor_msg.value["humidity"]
        logging.info(f"Received indoor humidity: {indoor_humidity}")

        # update shared data with indoor humidity and check if we should open the windows
        # use lock to prevent both threads from updating/reading shared data at the same time
        with lock:
            shared_data["indoor_humidity"] = indoor_humidity
            # if there is outdoor humidity data, check if we should open the windows
            if shared_data["outdoor_humidity"] is not None:
                # log the result if the window should be opened or closed
                logging.info(
                    should_open_windows(
                        shared_data["indoor_humidity"], shared_data["outdoor_humidity"]
                    )
                )
                shared_data["outdoor_humidity"] = (None)  # reset outdoor humidity data for next set of data
                

def consume_outdoor_humidity_messages(outdoor_consumer, shared_data, lock):
    for outdoor_msg in outdoor_consumer:
        # Get humidity and timestamp from message
        outdoor_humidity = outdoor_msg.value["humidity"]
        logging.info(f"Received outdoor humidity: {outdoor_humidity}")

        # update shared data with outdoor humidity and check if we should open the windows
        # use lock to prevent both threads from updating/reading shared data at the same time
        with lock:
            shared_data["outdoor_humidity"] = outdoor_humidity
            # if there is indoor humidity data, check if we should open the windows
            if shared_data["indoor_humidity"] is not None:
                # log the result if the window should be opened or closed
                logging.info(
                    should_open_windows(
                        shared_data["indoor_humidity"], shared_data["outdoor_humidity"]
                    )
                )
                shared_data["indoor_humidity"] = (None) # reset indoor humidity data for next set of data


def main():
    # Set up the kafka consumer
    kafka_config = KafkaConfig()
    indoor_consumer = setup_kafka_consumer(kafka_config, [INDOOR_TOPIC])
    outdoor_consumer = setup_kafka_consumer(kafka_config, [OUTDOOR_TOPIC])

    # Create a dict of shared data between threads and a lock for thread safety
    shared_data = {
        "indoor_humidity": None,
        "outdoor_humidity": None,
    }
    lock = threading.Lock()

    # Create the threads with the functions to consume the messages
    indoor_thread = threading.Thread(
        target=consume_indoor_humidity_messages,
        args=(indoor_consumer, shared_data, lock),
    )
    outdoor_thread = threading.Thread(
        target=consume_outdoor_humidity_messages,
        args=(outdoor_consumer, shared_data, lock),
    )

    # Start the threads and wait for them to finish
    indoor_thread.start()
    outdoor_thread.start()
    indoor_thread.join()
    outdoor_thread.join()


if __name__ == "__main__":
    # Run the main function in a loop
    while True:
        main()
