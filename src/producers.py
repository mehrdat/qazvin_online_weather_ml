from typing import Any, Optional

from time import sleep

from quixstreams import Application

from loguru import logger
import pandas as pd
import src.paths


class Buffer:
    def __init__(self,max_size:int):
        self.data = []
        self.max_size = max_size
        
    def append(self,data:Any):
        self.data.append(data)
        if len(self.data) > self.max_size:
            self.data.pop(0)
    def get_delayed_observation(self)->Any:
        if len(self.data) < self.max_size:
            return None
        return self.data[0]
    
    def __len__(self):
        return len(self.data)
    
    def __repr__(self) -> str:
        return str(self.data)
    
    

def run(
    path_to_live_data:str,
    kafka_broker_address:str,
    kafka_topic_features:str,
    kafka_topic_features_with_target:str,
    delay_target:int,
    target_name:Optional[str]="temp change ",
    
)-> None:
    app=Application(
        broker_address=kafka_broker_address,
        
    )
    topic_features=app.topic(name=kafka_topic_features,value_serializer="json")
    
    topic_features_with_target=app.topic(name=kafka_topic_features_with_target,value_serializer="json")
    
    data=pd.read_csv(path_to_live_data)
    
    features_names=[x for x in data.columns if x != target_name]
    
    data=data.to_dict(orient="records")
    buffer=Buffer(max_size=delay_target)
    
    with app.get_producer() as producer_of_features:
        with app.getproducer() as producer_of_features_with_targets:
            for d in data:
                features={k:v for k,v in d.items() if k in features_names}
                
                message=topic_features.serialize(key=None,value=features)
                producer_of_features.produce(
                    topic=topic_features.name,
                    key=message.key,
                    value=message.value
                )
                logger.info(f'message sent to kafka topic {topic_features.name}: {message.value}')  
                
                buffer.append(d)
                
                features_with_target=buffer.get_delayed_observation()
                
                if features_with_target is not None:
                    message=topic_features_with_target.serialize(
                        key=None,
                        value=features_with_target)
                    producer_of_features_with_targets.produce(
                        topic=topic_features_with_target.name,
                        key=message.key,
                        value=message.value
                    )
                    logger.info(f'message sent to kafka topic {topic_features_with_target.name}: {message.value}')
                sleep(1)

if '__name__' == '__main__':
    run(
        path_to_live_data=get_path_to_data_file("live.csv"),
        kafka_broker_address="localhost:9092",
        kafka_topic_features="features",
        kafka_topic_features_with_target="features_with_target",
        delay_target=5,
    )