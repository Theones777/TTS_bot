import os
import shutil

from utils.log import logging
from utils.variables import AVAIL_TXT_PROJECTS_NAMES, YD_HARD_HOMOGRAPHS_PATH, YD_DONE_NOT_ACCEPTED_HOMOGRAPHS_PATH, \
    YD_HARD_YOMOGRAPHS_PATH, YD_DONE_NOT_ACCEPTED_YOMOGRAPHS_PATH, TMP_DOWNLOAD_PATH, YD_BACKUP_PATH, TMP_ARC_PATH, \
    YD_ROOT_DICTORS_RAW_AUDIOS_PATH, AVAIL_CURATORS_PROJECTS
from utils.yd_dir.yd_init import y_disk


def get_text(message, marker_id, add_markup_marker_id, status, word, project_name):
    if add_markup_marker_id:
        file_name = f'{add_markup_marker_id}_{marker_id}_{word}_{status}.txt'
    else:
        file_name = f'{marker_id}_{word}_{status}.txt'

    if project_name == AVAIL_CURATORS_PROJECTS[0]:
        if status == 'hard':
            finded_file = f'{YD_HARD_HOMOGRAPHS_PATH}/{file_name}'
        else:
            finded_file = f'{YD_DONE_NOT_ACCEPTED_HOMOGRAPHS_PATH}/{file_name}'
    else:
        if status == 'hard':
            finded_file = f'{YD_HARD_YOMOGRAPHS_PATH}/{file_name}'
        else:
            finded_file = f'{YD_DONE_NOT_ACCEPTED_YOMOGRAPHS_PATH}/{file_name}'

    y_disk.download(finded_file, os.path.join(TMP_DOWNLOAD_PATH, file_name))
    meta_date_info = y_disk.get_meta(finded_file).created
    upload_date = f'{meta_date_info.day}.{meta_date_info.month}.{meta_date_info.year}'
    with open(os.path.join(TMP_DOWNLOAD_PATH, file_name), encoding='utf-8') as f:
        text = f.readlines()
    logging(message, f'{finded_file}_{upload_date}')
    os.remove(os.path.join(TMP_DOWNLOAD_PATH, file_name))

    return text, upload_date


def yd_doc_download(message, word, word_need_count, prev_marker_id, project_name=None):
    file_name = f'{prev_marker_id}_{word}_hard.txt'
    if project_name == AVAIL_TXT_PROJECTS_NAMES[1]:
        finded_file = f'{YD_HARD_HOMOGRAPHS_PATH}/{file_name}'
    elif project_name == AVAIL_TXT_PROJECTS_NAMES[2]:
        finded_file = f'{YD_HARD_YOMOGRAPHS_PATH}/{file_name}'
    else:
        finded_file = ''

    try:
        y_disk.download(finded_file, os.path.join(TMP_ARC_PATH, f'{word}_{word_need_count}.txt'))
        logging(message, finded_file)
        os.makedirs(os.path.join(YD_BACKUP_PATH, prev_marker_id), exist_ok=True)
        shutil.copy(os.path.join(TMP_ARC_PATH, f'{word}_{word_need_count}.txt'),
                    os.path.join(YD_BACKUP_PATH, prev_marker_id, f'{word}_{word_need_count}.txt'))
    except:
        with open(os.path.join(TMP_ARC_PATH, f'{word}_{word_need_count}.txt'), 'w') as f:
            f.write('')


def simple_download(inp_path, out_path):
    y_disk.download(inp_path, out_path)


def download_audio_files_from_yd(wav_path, user_id, dictor_name, text_type, wav_name):
    source_path = f'{YD_ROOT_DICTORS_RAW_AUDIOS_PATH}/{wav_path}'
    res_path = os.path.join(TMP_DOWNLOAD_PATH, user_id, dictor_name, text_type)
    os.makedirs(res_path, exist_ok=True)
    y_disk.download(source_path, os.path.join(res_path, wav_name))
