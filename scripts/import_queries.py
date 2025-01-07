import pandas as pd
import mysql.connector
from datetime import datetime
import os
from pathlib import Path
from dotenv import load_dotenv
import logging

class QueryDataImporter:
    def __init__(self):
        """Initialize the query data importer"""
        # Reuse the same structure as ShopifyDataImporter
        self.project_root = Path(__file__).parent.parent
        self.data_dir = self.project_root / 'data'
        self.raw_data_dir = self.data_dir / 'raw'
        self.logs_dir = self.project_root / 'logs'
        
        # Set up logging
        logging.basicConfig(
            filename=self.logs_dir / 'query_import.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # Load environment variables
        load_dotenv(self.project_root / '.env')
        
        self.db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': os.getenv('MYSQL_PASSWORD', 'qwert123'),
            'database': 'linoroso_analytics'
        }

    def import_queries(self):
        """Import search query data"""
        file_path = self.raw_data_dir / 'Queries.csv'
        
        if not file_path.exists():
            logging.error(f"Queries file not found: {file_path}")
            return
            
        try:
            df = pd.read_csv(file_path)
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            
            for index, row in df.iterrows():
                try:
                    sql = """
                        INSERT INTO search_queries 
                        (search_term, clicks, impressions, ctr, position, tracked_date)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    
                    ctr = float(row['CTR'].strip('%'))
                    values = (
                        row['Top queries'],
                        row['Clicks'],
                        row['Impressions'],
                        ctr,
                        row['Position'],
                        datetime.now().date()
                    )
                    
                    cursor.execute(sql, values)
                    
                    if index % 50 == 0:
                        conn.commit()
                        logging.info(f"Processed {index} queries")
                
                except Exception as row_error:
                    logging.error(f"Error processing query row {index}: {row_error}")
                    continue
                    
            conn.commit()
            logging.info("Query import completed successfully")
            
        except Exception as e:
            logging.error(f"Error during query import: {e}")
            if 'conn' in locals() and conn:
                conn.rollback()
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'conn' in locals() and conn:
                conn.close()
                logging.info("Database connection closed")

def main():
    """Main execution function"""
    try:
        importer = QueryDataImporter()
        importer.import_queries()
    except Exception as e:
        logging.error(f"Main execution error: {e}")
        raise

if __name__ == "__main__":
    main()