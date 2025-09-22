from asyncio.log import logger
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
import os
import sys
import queue
from dotenv import load_dotenv
load_dotenv()
import neo_api_client
from neo_api_client import NeoAPI
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime, time as datetime_time
import pandas as pandas
import time as time_module
import threading
import json
#TimeScaleDB
from psycopg2 import pool

class KotakTrader:
    
    def __init__(self):
        self.kotak_client = self.setup_login()
        self.db_pool = self.setup_db()
        if not self.db_pool:
            print("Failed to set up database connection")
            raise Exception("Database connection failed")
        self.setup_kafka()
        self.is_connecting = False
        

    def setup_login(self):
        try:
            #to load the session from file
            with open("creds.json", "r") as file:
                reuse_session = json.load(file)
                
            client = NeoAPI(access_token=reuse_session['access_token'], environment='prod', reuse_session=reuse_session)
            print("Reusing previous session")
            
        except (FileNotFoundError, json.JSONDecodeError, ValueError):
            # If file doesn't exist, is invalid, or has incomplete info, create a new session
            print("Creating new session")
            client = NeoAPI(consumer_key=os.getenv('CONSUMER_KEY'), consumer_secret=os.getenv('SECRET_KEY'), environment='prod')
            client.login(mobilenumber=os.getenv('MOBILE_NUMBER'), password=os.getenv('PASSWORD'))
            client.session_2fa(OTP=os.getenv('MPIN'))
            
            # Save the new session
            reuse_session = {
                'access_token': client.reuse_session['access_token'],
                'session_token': client.reuse_session['session_token'],
                'sid': client.reuse_session['sid'],
                'serverId': client.reuse_session['serverId']
            }
            with open("creds.json", "w") as file:
                file.write(json.dumps(reuse_session))

        return client

    def setup_db(self):
        try:
             db_pool = pool.SimpleConnectionPool(1, 20, os.getenv('DB_CONNECTION'), sslmode='require')
             return db_pool
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return None
    
    
    def set_callbacks(self):
        self.kotak_client.on_message = self.on_message
        self.kotak_client.on_error = self.on_error
        self.kotak_client.on_close = self.on_close
        self.kotak_client.on_open = self.on_open
    
    def insert_data(self, table, symbol, price):
        conn = self.db_pool.getconn()
        try:
            with conn.cursor() as curr:
                curr.execute(
                    f"""
                INSERT INTO {table} (time, symbol, price)
                VALUES (%s, %s, %s)
                """,
                    (datetime.now(), symbol, price)
                )
                conn.commit()
        except Exception as e:
            print(f"Error inserting data: {e}")
        finally:
            self.db_pool.putconn(conn)
            
    
    def process_message(self, message):
        logger.info("Processing message from Kafka")
        try:
            data = message
            if data.get("type") == "stock_feed":
                for item in data.get("data",[]):
                    symbol = item.get('ts') or item.get('tk')
                    current_price = item.get('ltp') or item.get('iv')
                    
                    if all([symbol, current_price]):
                        if symbol in ["Nifty 50", "Nifty Bank", "Nifty Fin Service"]:
                            self.insert_data("index_price_data", symbol, current_price)
                            print(symbol, current_price)
                        else:    
                            self.insert_data("symbol_price_data", symbol, current_price)
                            self.check_and_execute_trade(symbol, current_price)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
                    
    def check_and_execute_trade(self, symbol, current_price):   
        execute_trade()
        
    def log_trade(self, symbol, action, price):
        lof_order()
    
    def execute_trade(self, symbol, action, price):
        instant_execute()
    
    
    def on_open(self, message):  
        logger.info("WebSocket connection opened")
    
    
    def on_message(self, message):
        self.producer.send('market_data.', message)
        logger.info("Message put into queue")
            
    def on_error(self, error_message):
        print(f"WebSocket error: {error_message}")

    def on_close(self, message):
        print(f"WebSocket closed: {message}")
        current_time = datetime.now().time()
        market_close_time = datetime_time(15, 30)  # 3:30 PM

        if current_time < market_close_time:
            print("Attempting to reconnect...")
            self.reconnect()
        else:
            print("Market closed. Not attempting to reconnect.")
            
        

    def reconnect(self):
        if self.is_connecting:
            logger.info("Reconnection already in progress")
            return
        
        self.is_connecting = True
        max_retries = 5
        max_reconnection_time = 300  # 5 minutes
        start_time = time_module.time()

        try:
            for attempt in range(max_retries):
                if time_module.time() - start_time > max_reconnection_time:
                    logger.info("Maximum reconnection time reached. Stopping reconnection attempts.")
                    break

                try:
                    logger.info(f"Reconnection attempt {attempt + 1} of {max_retries}")
                    self.kotak_client = self.setup_login()
                    self.set_callbacks()
                    self.startWebSocket()
                    logger.info("Successfully reconnected")
                    return 
                except Exception as e:
                    logger.info(f"Reconnection attempt failed: {e}")
                    logger.info(f"Exception type: {type(e).__name__}")
                    logger.info(f"Exception details: {str(e)}")

                if attempt < max_retries - 1:
                    retry_delay = 10 * (2 ** attempt)  
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time_module.sleep(retry_delay)

            logger.info("Max retries reached. Unable to reconnect.")
        finally:
            self.is_connecting = False

    

    def startWebSocket(self):
        instrument_idx_tokens = [{"instrument_token": "Nifty 50", "exchange_segment": "nse_cm"},
                                 {"instrument_token": "Nifty Bank", "exchange_segment": "nse_cm"},
                                 {"instrument_token": "Nifty Fin Service", "exchange_segment": "nse_cm"}]
        instrument_stx_tokens = [{"instrument_token": "1333", "exchange_segment": "nse_cm"},
                                 {"instrument_token": "2885", "exchange_segment": "nse_cm"}]
        self.kotak_client.subscribe(instrument_tokens = instrument_idx_tokens, isIndex=True, isDepth=False)
        self.kotak_client.subscribe(instrument_tokens = instrument_stx_tokens, isIndex=False, isDepth=False)
        

    def run_data_processor(self):
        for message in self.consumer:
            self.process_message(message)   
            

    def run_bot(self):
        self.set_callbacks()
        websocket_thread = threading.Thread(target=self.startWebSocket)
        websocket_thread.start()
        wsprocessor_thread = threading.Thread(target=self.run_data_processor)
        wsprocessor_thread.start()

    

if __name__ == '__main__':
    traderX = KotakTrader()
    traderX.run_bot()