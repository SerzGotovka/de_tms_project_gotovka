import pandas as pd
import logging
import os



def save_csv_file(temp_file_path:str, data=None):
    """Функция для сохранения сгенерированных данных в csv"""
    df = pd.DataFrame(data)
    
    os.makedirs(os.path.dirname(temp_file_path), exist_ok=True)
    logging.info(f"Папка создана: {os.path.dirname(temp_file_path)}")

    df.to_csv(temp_file_path, index=False)
    logging.info(f'Файл создан: {temp_file_path}')



