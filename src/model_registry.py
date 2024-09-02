import pickle
import json 
from typing import Tuple

from river.compose import Pipeline
from src.paths import get_path_to_model

def push_model_to_registry(model:Pipeline,name:str,metadata:dict)->None:
    model_file=str(get_path_to_model(f'{name}.pkl'))
    with open(model_file,'wb') as f:
        pickle.dump(model,f)
    
    model_data_file=str(get_path_to_model(f'{name}.json'))
    
    with open(model_data_file,'w') as f:
        json.dump(metadata,f)
        
def load_model_from_registry(name:str)->Tuple[Pipeline,dict]:
    
    model_file=str(get_path_to_model(f'{name}.pkl'))
    
    with open(model_file,'rb') as f:
        model=pickle.load(f)
        
    model_data_file=str(get_path_to_model(f'{name}.json'))
    
    with open(model_data_file,'r') as f:
        metadata=json.load(f)
    
    return model,metadata
