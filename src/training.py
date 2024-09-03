from pathlib import Path    
import pandas as pd

import json
from datetime import datetime, timezone
from quixstreams import Application

from loguru import logger
from river.compat import River2SKLRegressor

from river import compose, linear_model, metrics, preprocessing

from src.date_utils import timestamp2utc
from src.model_registry import push_model_to_registry
from src.paths import get_path_to_data_file,get_path_to_model

import xgboost as xgb

# Create the XGBoost model
xgb_model = xgb.XGBRegressor()

def train_model_on_historical_data(path_tohistorical_data:Path):
    logger.info('Training model on historical data...')
    xgb_model = xgb.XGBRegressor()
    iver_xgb_model = River2SKLRegressor(xgb_model)
    model=compose.Pipeline(
        preprocessing.StandardScaler(),
        iver_xgb_model
    )
    
    for x in pd.read_csv(path_tohistorical_data,chunksize=1000):
        x=x.select_types(include=['number'])
        y=x.pop('target')
        
        model.learn_one(x,y)
    logger.info('Finished training initial model on historical data')
    last_timestamp=x.iloc[-1].to_dict()['timestamp']
    last_timestamp = timestamp2utc(last_timestamp)
    
    push_model_to_registry(
        model,
        name='model',
        metadata={
            'description': 'Initial model trained on historical data',
            'timestamp': last_timestamp,
        }
    )
    logger.info('Model saved!')
    return model

def train_model_incrementally_on_live_data(
    model:compose.Pipeline,
    kafka_browser_address:str,
    kafka_input_topic:str,
    kafka_consumer_group:str,
)->None:
    app=Application(
        broker_address=kafka_browser_address,
        consumer_group=kafka_consumer_group,
        auto_offset_reset='earliest',
    )
    topic=app.topic(name=kafka_input_topic,value_serializer='json')
    
    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[topic.name])
        
        while True:
            msg=consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                logger.error(f'Kafka Error: {msg.error()}')
                continue
            
            data=json.loads(msg.value().decode('utf-8'))
            x={k:v for k,v in data.items() if isinstance(v,(int,float))}
            
            y=x.pop('target')
            model.learn_one(x,y)
            
            logger.info(f'Updated model with new data point at {datetime.now(timezone.utc)}')
            model_timestamp=timestamp2utc(data['timestamp'])
            
            push_model_to_registry(
                model,
                name='model',
                metadata={
                    'description':'Model trained incrementally on live data',
                    'timestamp':model_timestamp,
                }
            )
            logger.info('Model saved!') 
            
def train(
    path_to_historical_data:Path,
    kafka_broker_address:str,
    kafka_input_topic:str,
    kafka_consumer_group:str,
)->None:
    model=train_model_on_historical_data(
        path_to_historical_data
    )
    logger.info('Model trained on historical data')
    train_model_incrementally_on_live_data(
        model,
        kafka_broker_address,
        kafka_input_topic,
        kafka_consumer_group,
    )

if __name__=='__main__':
    train(
        path_to_historical_data=get_path_to_data_file('historical_data.csv'),
        kafka_broker_address='localhost:9092',
        kafka_input_topic='features_with_target',
        kafka_consumer_group='training_consumer_group',
    )
    