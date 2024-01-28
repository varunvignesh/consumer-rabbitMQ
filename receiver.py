import pika
import sys
import os
import json
import sqlite3
import ast
import logging
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO)

# Get the .env variables
load_dotenv()

# Initialize connection
with sqlite3.connect(os.getenv("DATABASE_NAME")) as conn:

    # use cursor for commits
    cursor= conn.cursor()

    # rabbitMQ connection
    with pika.BlockingConnection(pika.ConnectionParameters(os.getenv('RABBITMQ_HOST'))) as connection:
        channel = connection.channel()
        
        queue_name = os.getenv('RABBITMQ_QUEUE_NAME')

        # declare the queue
        channel.queue_declare(queue=queue_name)

        def callback(ch, method, properties, body):
            try: 
                # convert string to json object/dict
                received_data = json.loads(body)
                
                # using ast as the json data have single quotes string, else would've used json as that's specifically designed for efficient JSON processing.
                meta_info = ast.literal_eval(received_data.get('meta_info'))

                # Start a transaction
                cursor.execute("BEGIN")

                # insert if not exists values in account_db
                cursor.execute(
                    """     
                            INSERT OR IGNORE INTO account_db (account_code) VALUES (?)
                    """, 
                    (
                        meta_info.get("account_code"),
                    )
                )

                # fetch account_id from account_db
                cursor.execute("SELECT id FROM account_db WHERE account_code = ?", (meta_info.get("account_code"),))
                
                account_id = cursor.fetchone()

                # raise exception if account_code does not exists
                if account_id is None:
                    raise Exception('account_code: {} not found'.format(meta_info.get("account_code")))
                else:
                    account_id = account_id[0]

                # Note: could use bulk insert for perfomarce, if the case arises.
                # insert values for product_db.
                cursor.execute(
                    """     
                            INSERT INTO product_db (account_id, reference_product_id, stock) VALUES (?, ?, ?)
                    """, 
                    (
                        account_id,
                        meta_info.get("reference_product_id"),
                        received_data["stock"],
                    )
                )

                logging.info('Inserted into product_db id: {}'.format(cursor.lastrowid))
                
                # Commit the transaction
                cursor.execute("COMMIT")
            
            except Exception as e:
                # Rollback the transaction on exception
                cursor.execute("ROLLBACK")
                
                # Log the exception
                logging.error(f"Error: {e}")
        
        # callback declaring
        channel.basic_consume(queue=queue_name, auto_ack=True, on_message_callback=callback)
        
        logging.info(' [{}] Started listening on the queue'.format(queue_name))
       
        try:
            # start listening on the channel
            channel.start_consuming()

        except KeyboardInterrupt:
            logging.info('Interrupted. Closing connection...')
            channel.stop_consuming()

logging.info('Script execution completed.')
