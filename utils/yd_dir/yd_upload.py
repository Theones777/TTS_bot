import os
import shutil

import yadisk

from utils.checks.check import convert_to_utf8
from utils.log import logging
from utils.variables import AVAIL_TXT_PROJECTS_NAMES, YD_HARD_HOMOGRAPHS_PATH, YD_DONE_NOT_ACCEPTED_HOMOGRAPHS_PATH, \
    YD_DONE_NOT_ACCEPTED_YOMOGRAPHS_PATH, YD_HARD_YOMOGRAPHS_PATH, TMP_DOWNLOAD_PATH, USER_BACKUP_PATH, \
    YD_DONE_ACCEPTED_HOMOGRAPHS_PATH, YD_DONE_ACCEPTED_YOMOGRAPHS_PATH, AVAIL_AUDIO_PROJECTS_NAMES, \
    YD_ROOT_DICTORS_DONE_AUDIOS_PATH, YD_ROOT_DICTORS_RESERVE_AUDIOS_PATH
from utils.yd_dir.yd_init import y_disk


def move_file_to_yd(message, project_name, file_name, sample, marker_id, mark_done, prev_marker_id):
    result_path = remove_path = ''
    if project_name == AVAIL_TXT_PROJECTS_NAMES[0]:
        if mark_done == 'hard':
            result_path = YD_HARD_HOMOGRAPHS_PATH + f'/{marker_id}_{sample}_{mark_done}.txt'
        else:
            result_path = YD_DONE_NOT_ACCEPTED_HOMOGRAPHS_PATH + f'/{marker_id}_{sample}_{mark_done}.txt'

    elif project_name == AVAIL_TXT_PROJECTS_NAMES[1]:
        result_path = YD_DONE_NOT_ACCEPTED_HOMOGRAPHS_PATH + f'/{marker_id}_{prev_marker_id}_{sample}_{mark_done}.txt'
        remove_path = YD_HARD_HOMOGRAPHS_PATH + f'/{prev_marker_id}_{sample}_hard.txt'

    elif project_name == AVAIL_TXT_PROJECTS_NAMES[2]:
        result_path = YD_DONE_NOT_ACCEPTED_YOMOGRAPHS_PATH + f'/{marker_id}_{prev_marker_id}_{sample}_{mark_done}.txt'
        remove_path = YD_HARD_YOMOGRAPHS_PATH + f'/{prev_marker_id}_{sample}_hard.txt'

    try:
        convert_to_utf8(os.path.join(TMP_DOWNLOAD_PATH, marker_id, file_name))
        logging(message, result_path)
        y_disk.upload(os.path.join(TMP_DOWNLOAD_PATH, marker_id, file_name), result_path)
        logging(message, 'Успешная загрузка на ЯД')
        if project_name in AVAIL_TXT_PROJECTS_NAMES[1:]:
            try:
                y_disk.remove(remove_path, permanently=True)
            except yadisk.exceptions.PathNotFoundError:
                pass
    except yadisk.exceptions.ParentNotFoundError:
        os.remove(os.path.join(TMP_DOWNLOAD_PATH, marker_id, file_name))
        return 'Нужная папка не найдена (проверьте корректность выбора проекта)'
    except yadisk.exceptions.PathExistsError:
        os.remove(os.path.join(TMP_DOWNLOAD_PATH, marker_id, file_name))
        return f'Файл {file_name} уже загружен!'

    os.makedirs(os.path.join(USER_BACKUP_PATH, marker_id), exist_ok=True)
    shutil.copy(os.path.join(TMP_DOWNLOAD_PATH, marker_id, file_name),
                os.path.join(USER_BACKUP_PATH, marker_id, file_name))
    os.remove(os.path.join(TMP_DOWNLOAD_PATH, marker_id, file_name))

    str_to_return = f'Файл {file_name} загружен в {project_name}'

    return str_to_return


def upload_to_yd(project_name, download_file_path, file_name):
    out_str = ''
    if project_name in AVAIL_TXT_PROJECTS_NAMES[:2]:
        dones_path = f'{YD_DONE_ACCEPTED_HOMOGRAPHS_PATH}/{file_name}'
    elif project_name == AVAIL_TXT_PROJECTS_NAMES[2]:
        dones_path = f'{YD_DONE_ACCEPTED_YOMOGRAPHS_PATH}/{file_name}'
    elif project_name == AVAIL_AUDIO_PROJECTS_NAMES[0]:
        dones_path = f'{YD_ROOT_DICTORS_DONE_AUDIOS_PATH}/{file_name}'

    try:
        if download_file_path.endswith('txt'):
            convert_to_utf8(download_file_path)
        y_disk.upload(download_file_path, dones_path)
        out_str += f'Файл {file_name} загружен на Яндекс диск\n'
        status = 1
    except yadisk.exceptions.PathExistsError:
        try:
            y_disk.upload(download_file_path, f'{YD_ROOT_DICTORS_RESERVE_AUDIOS_PATH}/{file_name}')
            out_str += f'{file_name} загружен в резерв\n'
        except:
            out_str += f'{file_name} уже загружен!\n'
        os.remove(download_file_path)
        status = 0
    return out_str,  status
