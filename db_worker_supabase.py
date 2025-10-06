from supabase import create_client
from queue import Queue
from time import time

class DBWorkerSupabase:
    def __init__(self, db_url: str, db_key: str, queue: Queue):
        self.db_url = db_url
        self.db_key = db_key
        self.queue = queue
        self.supabase = create_client(self.db_url, self.db_key)
        self.previous_uns = {}

    def insert_factory_message(self, factory_name: str, unique_id: str, json_payload: dict):
        """
        Insert into history and upsert into current.
        factory_name: name of the factory
        unique_id: unique identifier for the current record
        json_payload: the full MQTT JSON
        """
        # --- Insert into history ---
        

        if(self.previous_uns != json_payload):

            self.previous_uns = json_payload

            try:
                self.supabase.table('uns_lvl_factory_history').insert({
                    'factory': factory_name,
                    'received_uns': json_payload
                }).execute()
            except Exception as e:
                print(f"Failed to insert into history: {e}")

            # --- Upsert into current ---
            try:
                self.supabase.table('uns_lvl_factory_current').upsert({
                    'factory': factory_name,
                    'received_uns': json_payload
                }, on_conflict=['factory']).execute()
            except Exception as e:
                print(f"Failed to upsert into current: {e}")

            print(f"Inserted/upserted factory {factory_name} | unique_id: {unique_id}")

        else:
            print("Duplicate factory message, ignoring.")

    def run(self):
        while True:
            topic, payload = self.queue.get()
            try:
                # Extract factory name and its data
                factory_name = list(payload.keys())[0]
                factory_data = payload[factory_name]
                unique_id = factory_name + "_" + str(int(time()*1000))
                
                # Insert into history & upsert into current
                self.insert_factory_message(factory_name, unique_id, factory_data)
            except Exception as e:
                print(f"DB error: {e}")
            finally:
                self.queue.task_done()

