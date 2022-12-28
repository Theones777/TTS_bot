import datetime
import os

from aiogram import types

from utils.variables import LOGS_PATH, SUM_PROFILES


def logging(message: types.Message, add_str=''):
    marker_id = SUM_PROFILES[str(message.from_user.id)]
    today = datetime.datetime.today().isoformat(sep=" ").split(' ')[0]
    to_time = str(datetime.datetime.today().isoformat(sep=" ").split(' ')[1].split('.')[0])
    log_str = to_time + '-' + marker_id
    if message.document:
        log_str += f'- {message.document.file_name} - {message.document.file_size} байт - {message.document.file_id} - \
                    {message.document.file_unique_id}\n'
    elif message.text:
        log_str += f'- {message.text} - {add_str}\n'

    with open(os.path.join(LOGS_PATH, f'log_{today}.txt'), 'a', encoding='utf-8') as f:
        f.write(log_str)
