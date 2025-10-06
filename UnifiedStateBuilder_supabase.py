from datetime import datetime
from supabase import create_client
import json

class UnifiedStateBuilderSupabase:
    def __init__(self, db_url: str, db_key: str):
        self.supabase = create_client(db_url, db_key)

    def build_enterprise_uns(self):
        """Build a single enterprise UNS from all factories"""
        # Fetch all current factory-level UNS
        try:
            response = (
                self.supabase
                    .table('uns_lvl_factory_current')
                    .select('factory, received_uns')
                    .execute()
            )

            if response:
               print("Fetching factories")
            else:
                print("Insert may have failed, got:", response)
                return None
           

            rows = response.data
            self.enterprise_uns = {}

            for row in rows:
                factory_name = row['factory']
                factory_json = row['received_uns']

                # Merge each factory under its name
                self.enterprise_uns[factory_name] = factory_json
        except Exception as e:
            print("Error building enterprise UNS:", e)
            return None
        return self.enterprise_uns

    