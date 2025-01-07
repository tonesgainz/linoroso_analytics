import pandas as pd
import mysql.connector
from datetime import datetime
import os
from pathlib import Path
from dotenv import load_dotenv
import logging

class ShopifyDataImporter:
    def __init__(self):
        """Initialize the data importer with project paths and logging"""
        # Get the project root directory by going up one level from 'scripts'
        self.project_root = Path(__file__).parent.parent
        
        # Set up paths for different directories
        self.data_dir = self.project_root / 'data'
        self.raw_data_dir = self.data_dir / 'raw'
        self.processed_data_dir = self.data_dir / 'processed'
        self.logs_dir = self.project_root / 'logs'
        
        # Create directories if they don't exist
        self.processed_data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)
        
        # Set up logging
        logging.basicConfig(
            filename=self.logs_dir / 'import.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # Load environment variables from .env file
        load_dotenv(self.project_root / '.env')
        
        # Database connection configuration
        self.db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': os.getenv('MYSQL_PASSWORD', 'qwert123'),
            'database': 'linoroso_analytics'
        }
        
        logging.info("ShopifyDataImporter initialized successfully")

    def connect_db(self):
        """Create a database connection with error handling"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            logging.info("Successfully connected to database")
            return connection
        except mysql.connector.Error as err:
            logging.error(f"Database connection failed: {err}")
            raise
    
    def import_products(self):
        """Import product data from CSV file"""
        file_path = self.raw_data_dir / 'products_export_12.csv'
        
        if not file_path.exists():
            logging.error(f"Product file not found: {file_path}")
            return
        
        try:
            logging.info(f"Reading product data from {file_path}")
            df = pd.read_csv(file_path)
            logging.info(f"Successfully read CSV file with {len(df)} rows")
            
            # Save a processed copy with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            processed_path = self.processed_data_dir / f'products_processed_{timestamp}.csv'
            df.to_csv(processed_path, index=False)
            
            conn = self.connect_db()
            cursor = conn.cursor()
            
            # Process each product row
            for index, row in df.iterrows():
                try:
                    self._process_product_row(cursor, row, index)
                    
                    # Commit every 50 records
                    if index % 50 == 0:
                        conn.commit()
                        logging.info(f"Processed {index} products")
                
                except Exception as row_error:
                    logging.error(f"Error processing row {index}: {row_error}")
                    continue
            
            # Final commit
            conn.commit()
            logging.info("Product import completed successfully")
            
        except Exception as e:
            logging.error(f"Error during import process: {e}")
            if 'conn' in locals() and conn:
                conn.rollback()
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'conn' in locals() and conn:
                conn.close()
                logging.info("Database connection closed")
    
    def _process_product_row(self, cursor, row, index):
        """Process a single product row - helper method"""
        # Update products table
        product_sql = """
            INSERT INTO products 
            (product_id, title, product_type, vendor, price, compare_price, 
             status, inventory_policy)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            product_type = VALUES(product_type),
            vendor = VALUES(vendor),
            price = VALUES(price),
            compare_price = VALUES(compare_price),
            status = VALUES(status),
            inventory_policy = VALUES(inventory_policy)
        """
        
        product_values = (
            row['Handle'],
            row['Title'],
            row['Type'],
            row['Vendor'],
            float(row['Variant Price']) if pd.notna(row['Variant Price']) else 0.0,
            float(row['Variant Compare At Price']) if pd.notna(row['Variant Compare At Price']) else 0.0,
            row['Status'],
            row['Variant Inventory Policy']
        )
        
        cursor.execute(product_sql, product_values)
        
        # Add record to products_history
        history_sql = """
            INSERT INTO products_history 
            (product_id, title, price, compare_price, inventory_quantity,
             status, vendor, product_type, tags)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        history_values = (
            row['Handle'],
            row['Title'],
            float(row['Variant Price']) if pd.notna(row['Variant Price']) else 0.0,
            float(row['Variant Compare At Price']) if pd.notna(row['Variant Compare At Price']) else 0.0,
            0,  # Default inventory quantity
            row['Status'],
            row['Vendor'],
            row['Type'],
            row.get('Tags', '')
        )
        
        cursor.execute(history_sql, history_values)

def main():
    """Main execution function"""
    try:
        importer = ShopifyDataImporter()
        importer.import_products()
    except Exception as e:
        logging.error(f"Main execution error: {e}")
        raise

if __name__ == "__main__":
    main()