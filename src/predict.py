from typing import Optional
import json

from quixstreams import Application
from loguru import logger

from src.model_registry import load_model_from_registry

def run(
    kafka_broker_address:str,
    kafka_input_topic:str,
    kafka_consumer_group:str,
    refresh_model: Optional[bool] = False,
)->None:
    app=Application(
        broker_address=kafka_broker_address,
        consumer_group=kafka_consumer_group
    )
    
    topic=app.topic(name=kafka_input_topic,value_deserializer='json')
    
    model,model_date=load_model_from_registry(name='model')
    logger.info(f'Using model: ', model_date)
    
    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[topic.name])
        
        while True:
            msg=consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                logger.error(f'Kafka Error: {msg.error()}')
                continue
            
            data=json.loads(msg.values().decode('utf-8'))
            
            x={k:v for k,v in data.items() if isinstance(v,(int,float))}
            
            y_pred=model.predict_one(x)
            logger.info(f'Predicted target: {y_pred}')
            
            consumer.store_offset(message=msg)
            
            if refresh_model:
                try:
                    model,model_data=load_model_from_registry(name='model')
                    logger.info(f'Using model: {model_data}')
                except Exception as e:
                    #logger.error(f'Error loading model: {e}')
                    continue

if "__name__"=="__main__":
    run(
        kafka_broker_address='localhost:9092',
        kafka_input_topic='features',
        kafka_consumer_group='prediction_consumer_group',
        refresh_model=True,
    )