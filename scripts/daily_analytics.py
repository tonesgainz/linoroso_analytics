Now, let's create a script to help you get started each day. Save this as `daily_analytics.py` in your scripts folder:

```python
import logging
from pathlib import Path
import subprocess
from datetime import datetime
import mysql.connector
from dotenv import load_dotenv
import os

class DailyAnalytics:
    def __init__(self):
        """Initialize daily analytics process"""
        self.project_root = Path(__file__).parent.parent
        self.setup_logging()
        load_dotenv(self.project_root / '.env')
        
        self.db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': 'linoroso_analytics'
        }

    def setup_logging(self):
        """Set up logging for daily operations"""
        log_file = self.project_root / 'logs' / f'daily_{datetime.now().strftime("%Y%m%d")}.log'
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def run_daily_imports(self):
        """Run all daily import scripts"""
        try:
            # Run product import
            logging.info("Starting product import...")
            subprocess.run(['python', 'scripts/import_shopify_data.py'], check=True)
            
            # Run query import
            logging.info("Starting query import...")
            subprocess.run(['python', 'scripts/import_queries.py'], check=True)
            
        except subprocess.CalledProcessError as e:
            logging.error(f"Error running imports: {e}")
            raise

    def verify_imports(self):
        """Verify today's data imports"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Check today's imports
            verification_queries = {
                'products': """
                    SELECT COUNT(*) 
                    FROM products_history 
                    WHERE DATE(recorded_date) = CURRENT_DATE()
                """,
                'queries': """
                    SELECT COUNT(*) 
                    FROM search_queries 
                    WHERE DATE(tracked_date) = CURRENT_DATE()
                """
            }
            
            results = {}
            for name, query in verification_queries.items():
                cursor.execute(query)
                count = cursor.fetchone()[0]
                results[name] = count
                logging.info(f"Today's {name} count: {count}")
            
            return results
            
        except Exception as e:
            logging.error(f"Verification error: {e}")
            raise
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()

def main():
    """Run daily analytics process"""
    analytics = DailyAnalytics()
    
    try:
        # Run imports
        analytics.run_daily_imports()
        
        # Verify imports
        results = analytics.verify_imports()
        
        # Log summary
        logging.info("Daily analytics completed successfully")
        logging.info(f"Import summary: {results}")
        
    except Exception as e:
        logging.error(f"Daily analytics failed: {e}")
        raise

if __name__ == "__main__":
    main()