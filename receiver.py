import pika, sys, os, json, sqlite3, ast
from dotenv import load_dotenv

# Get the .env variables
load_dotenv()

# Initialize connection
conn = sqlite3.connect(os.getenv("DATABASE_NAME"))

# use cursor for commits
cursor= conn.cursor()

def main():
    # rabbitMQ connection
    connection = pika.BlockingConnection(pika.ConnectionParameters(os.getenv('RABBITMQ_HOST')))
    channel = connection.channel()

    queue_name = os.getenv('RABBITMQ_QUEUE_NAME')

    # creating a queue if not exists
    channel.queue_declare(queue=queue_name)

    def callback(ch, method, properties, body):

        # convert string to json object/dict
        received_data = json.loads(body)
        
        # using ast as the json data have single quotes string, else would've used json as that's specifically designed for efficient JSON processing.
        meta_info = ast.literal_eval(received_data.get('meta_info'))

        # Note: could use bulk insert for perfomarce, if the case arises.
        # insert values for product_db
        cursor.execute(
            """     
                    INSERT INTO product_db (index_number, available_price, stock, source, reference_product_id) VALUES (?, ?, ?, ?, ?)
            """, 
            (
                received_data["index"],
                received_data["available_price"],
                received_data["stock"],
                received_data["source"],
                meta_info.get("reference_product_id")
            )
        )

        # fetch reference key the table meta_info_db
        product_info_id = cursor.lastrowid      

        # insert values for meta_info_db
        cursor.execute("""
                    INSERT INTO meta_info_db (product_info_id, account_code, crawl_page_counter, postal_zip_code, postal_zip_name, store_code, place_name, admin_name, bundle_versions_row_pk_hash, major_version_end_time, bundle_variant_field_mapping, bundle_definition, fulfilment_modes, seller_name, bundle_match_type, bundle_definition_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, 
                (
                    product_info_id,
                    meta_info.get("account_code"),
                    meta_info.get("crawl_page_counter"),
                    meta_info.get("postal_zip_code"),
                    meta_info.get("postal_zip_name"),
                    meta_info.get("store_code"),
                    meta_info.get("place_name"),
                    meta_info.get("admin_name1"),
                    meta_info.get("bundle_versions_row_pk_hash"),
                    meta_info.get("major_version_end_time"),
                    json.dumps(meta_info.get("bundle_variant_field_mapping")),
                    json.dumps(meta_info.get("bundle_definition")),
                    json.dumps(meta_info.get("fulfilment_modes")),
                    meta_info.get("seller_name"),
                    meta_info.get("bundle_match_type"),
                    meta_info.get("bundle_definition_hash"),
                ))

        print('Inserted into meta_info_db id: {}'.format(cursor.lastrowid))
        
        # commit connection
        conn.commit()
    
    # callback declaring
    channel.basic_consume(queue=queue_name, auto_ack=True, on_message_callback=callback)
    
    print(' [{}] Started listening on the queue'.format(queue_name))
    # start listening on the channel
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    # if stopped manually close connection
    except KeyboardInterrupt:
        conn.close()
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
