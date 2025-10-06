import datetime
import os
import dotenv
import threading
from queue import Queue

from supabase import create_client
from mqtt_subscriber import MQTTSubscriber
# from db_worker import DBWorker
# import UnifiedStateBuilder
import time
# from mqtt_publisher import MQTTPublisher
from db_worker_supabase import DBWorkerSupabase
from UnifiedStateBuilder_supabase import UnifiedStateBuilderSupabase
import json


dotenv.load_dotenv()

class MainApp:
    # def __init__(self, mqtt_broker, db_config):
    def __init__(self, mqtt_broker):
        self.supabase = create_client(os.getenv("SUPABASE_URL_ENTERPRISE"), os.getenv("SUPABASE_KEY_ENTERPRISE"))

        self.queue = Queue()
        self.subscriber = MQTTSubscriber(mqtt_broker, self.queue)


        self.db_worker = DBWorkerSupabase(os.getenv("SUPABASE_URL_ENTERPRISE"), os.getenv("SUPABASE_KEY_ENTERPRISE"), self.queue)
        # self.db_worker = DBWorker(db_config, self.queue)
        # self.UnifiedStateBuilder = UnifiedStateBuilder.UnifiedStateBuilder(db_config)
        self.UnifiedStateBuilder = UnifiedStateBuilderSupabase(os.getenv("SUPABASE_URL_ENTERPRISE"), os.getenv("SUPABASE_KEY_ENTERPRISE"))
        # self.publisher = MQTTPublisher(mqtt_broker)

    
    def timed_publish(self, interval):
        self.last_enterprise_uns = {}
        
        while True:
            try:
                enterprise_uns = self.UnifiedStateBuilder.build_enterprise_uns()

                if enterprise_uns != self.last_enterprise_uns:

                    self.last_enterprise_uns = enterprise_uns

                    self.supabase.table('unified_json_history').insert({'uns': enterprise_uns}).execute()

                    print("Published unified namespace to db")
                else:
                    print("No changes in unified namespace, not publishing to enterprise db")
            except Exception as e:
                print("Failed to publish enterprise UNS:", e)

            time.sleep(interval)  # wait for 'interval' seconds



    def start(self):
        # Run DB worker in background thread
        threading.Thread(target=self.db_worker.run, daemon=True).start()

        # Run timed_publish every 5 seconds in background thread
        threading.Thread(target=self.timed_publish, args=(5,), daemon=True).start()

        # Run MQTT subscriber (blocking)
        self.subscriber.run()

if __name__ == "__main__":
    # app = MainApp("broker.mqtt.cool", db_config)
    app = MainApp("broker.mqtt.cool")
    app.start()
