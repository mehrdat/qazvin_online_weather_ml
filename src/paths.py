from pathlib import Path

def get_path_to_data_file(file_name:str)-> Path:
    path_to_this_file=Path(__file__)
    parent_directory=path_to_this_file.parent.parent
    
    path_to_data=parent_directory/'data'/file_name
    return path_to_data


def get_path_to_model(file_name:str)->Path:
    path_to_this_file=Path(__file__)
    parent_directory=path_to_this_file.parent.parent
    path_to_models=parent_directory/'models'/file_name
    return path_to_models

