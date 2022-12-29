import datetime
import os
import re
import shutil
import warnings
import zipfile

import pandas as pd
from gspread_dataframe import set_with_dataframe

from utils.log import logging
from utils.tables.google_sheets import gc
from utils.variables import TMP_ARC_PATH, AVAIL_AUDIO_PROJECTS_NAMES, LONG_AUDIOS_CSV, \
    TMP_DOWNLOAD_PATH, IDX_FILENAME_COL, AVAIL_TXT_PROJECTS_NAMES, CURATOR_HOMOGRAPH_CSV, \
    YD_DONE_NOT_ACCEPTED_HOMOGRAPHS_PATH, CURATOR_YOMOGRAPH_CSV, YD_DONE_NOT_ACCEPTED_YOMOGRAPHS_PATH, \
    MARKERS_SOUND_CSV, DICTORS_TEXTS_PATH, \
    YD_ROOT_DICTORS_DONE_AUDIOS_PATH, DICTORS_TABLE_NAME, DICTORS_WORKSHEET_NAME
from utils.yd_dir.yd_download import simple_download, download_audio_files_from_yd
from utils.yd_dir.yd_init import y_disk
from utils.yd_dir.yd_upload import upload_to_yd

warnings.simplefilter(action='ignore', category=FutureWarning)


def get_curator_words(message, curator_id, project_name):
    words_num = int(message.text)
    today = datetime.datetime.today().isoformat(sep=" ").split(' ')[0]
    arc_path = os.path.join(TMP_ARC_PATH, f'{today}_{curator_id}.zip')
    if project_name in AVAIL_TXT_PROJECTS_NAMES[:2]:
        csv_path = CURATOR_HOMOGRAPH_CSV
        dones_path = YD_DONE_NOT_ACCEPTED_HOMOGRAPHS_PATH
    else:
        csv_path = CURATOR_YOMOGRAPH_CSV
        dones_path = YD_DONE_NOT_ACCEPTED_YOMOGRAPHS_PATH

    df = pd.read_csv(csv_path)
    tmp_df = df[df['curator_id'] == 'none']
    if len(tmp_df) < words_num:
        for f in y_disk.listdir(dones_path):
            if f.name not in df['file_name'].tolist():
                df = df.append({'file_name': f.name,
                                'upload_date': f.created,
                                'curator_id': 'none'}, ignore_index=True)
        df.sort_values('upload_date', ascending=True, inplace=True, ignore_index=True)
        df.to_csv(csv_path, index=False)
        tmp_df = df[df['curator_id'] == 'none']
    output_files_names = tmp_df.iloc[0:words_num, IDX_FILENAME_COL].tolist()

    with zipfile.ZipFile(arc_path, 'w') as myzip:
        for filename in output_files_names:
            logging(message, filename)
            simple_download(f'{dones_path}/{filename}', os.path.join(TMP_ARC_PATH, filename))
            myzip.write(os.path.join(TMP_ARC_PATH, filename), arcname=filename)
            os.remove(os.path.join(TMP_ARC_PATH, filename))
            df.loc[df['file_name'] == filename, 'curator_id'] = curator_id
    df.to_csv(csv_path, index=False)

    return arc_path


def get_audio_data(message, user_id):
    project_name = message.text
    today = datetime.datetime.today().isoformat(sep=" ").split(' ')[0]
    arc_path = os.path.join(TMP_ARC_PATH, f'{today}_{user_id}.zip')

    if project_name == AVAIL_AUDIO_PROJECTS_NAMES[0]:
        long_df = pd.read_csv(LONG_AUDIOS_CSV)
        tmp_long_df = long_df[long_df['status'] == 'waiting']
        wav_path = tmp_long_df.iloc[0, IDX_FILENAME_COL]
        long_df.loc[long_df['file_name'] == wav_path, 'status'] = 'coping'
        long_df.to_csv(LONG_AUDIOS_CSV, index=False)
        os.makedirs(os.path.join(TMP_DOWNLOAD_PATH, user_id), exist_ok=True)

        with zipfile.ZipFile(arc_path, 'w') as myzip:
            dictor_name = wav_path.split('/')[0]
            text_type = wav_path.split('/')[1]
            wav_name = wav_path.split('/')[2]
            logging(message, wav_path)
            download_audio_files_from_yd(wav_path, user_id, dictor_name, text_type, wav_name)
            logging(message, 'Загрузка успешна')
            myzip.write(os.path.join(TMP_DOWNLOAD_PATH, user_id, dictor_name, text_type, wav_name),
                        arcname=f'{dictor_name}_{text_type}_{wav_name}')

            start_idx = int(wav_name.split('_')[1].split('-')[0]) - 1
            end_idx = int(wav_name.split('_')[1].split('-')[1].split('.')[0])
            with open(os.path.join(DICTORS_TEXTS_PATH, f'{text_type}.txt'), encoding='utf-8') as f:
                common_text = f.readlines()
                text = common_text[start_idx:end_idx]

            txt_name = os.path.join(TMP_DOWNLOAD_PATH, user_id, dictor_name, text_type,
                                    wav_name.replace('.wav', '.txt'))
            with open(txt_name, 'w', encoding='utf-8') as f:
                f.writelines(text)
            myzip.write(txt_name, arcname=f"{dictor_name}_{text_type}_{wav_name.replace('.wav', '.txt')}")

        marker_df = pd.read_csv(MARKERS_SOUND_CSV)
        for text_i, i in enumerate(range(start_idx + 1, end_idx + 1)):
            new_wav_name = f'{wav_name.split("_")[0]}_{i}.wav'
            marker_df = marker_df.append({'file_name': f'{dictor_name}/{text_type}/{new_wav_name}',
                                          'status': 'in_process',
                                          'marker_id': user_id,
                                          'original_text': text[text_i]}, ignore_index=True)

        long_df.loc[long_df['file_name'] == wav_path, 'status'] = 'in_process'
        long_df.loc[long_df['file_name'] == wav_path, 'marker_id'] = user_id
        long_df.to_csv(LONG_AUDIOS_CSV, index=False)
        marker_df.to_csv(MARKERS_SOUND_CSV, index=False)
        shutil.rmtree(TMP_DOWNLOAD_PATH)

        timetable = gc.open(DICTORS_TABLE_NAME)
        time_worksheet = timetable.worksheet(DICTORS_WORKSHEET_NAME)
        set_with_dataframe(time_worksheet, marker_df)
    return arc_path


def enter_audio_data(message, user_id, project_name):
    out_str = ''
    if project_name == AVAIL_AUDIO_PROJECTS_NAMES[0]:
        files_path = os.path.join(TMP_DOWNLOAD_PATH, user_id, 'unzip_files')
        today = datetime.datetime.today().isoformat(sep=" ").split(' ')[0]
        upload_date = '.'.join([today.split('-')[2], today.split('-')[1], today.split('-')[0]])

        dictor_name = os.listdir(files_path)[0].split('_')[0]
        text_type = os.listdir(files_path)[0].split('_')[1]
        dictors_dirs = [dd.name for dd in y_disk.listdir(YD_ROOT_DICTORS_DONE_AUDIOS_PATH)]
        if dictor_name not in dictors_dirs:
            y_disk.mkdir(f'{YD_ROOT_DICTORS_DONE_AUDIOS_PATH}/{dictor_name}')
        text_type_dirs = [ttd.name for ttd in y_disk.listdir(f'{YD_ROOT_DICTORS_DONE_AUDIOS_PATH}/{dictor_name}')]
        if text_type not in text_type_dirs:
            y_disk.mkdir(f'{YD_ROOT_DICTORS_DONE_AUDIOS_PATH}/{dictor_name}/{text_type}')

        long_df = pd.read_csv(LONG_AUDIOS_CSV)
        marker_df = pd.read_csv(MARKERS_SOUND_CSV)
        txt_file = [f for f in os.listdir(files_path) if f.endswith('.txt')][0]
        long_df.loc[
            long_df[
                'file_name'] == f'{dictor_name}/{text_type}/{"_".join(txt_file.split("_")[2:]).replace(".txt", ".wav")}',
            'status'] = 'done'
        long_df.to_csv(LONG_AUDIOS_CSV, index=False)
        with open(os.path.join(files_path, txt_file), encoding='utf-8') as f:
            marked_texts = f.readlines()
        for file in os.listdir(files_path):
            if file.endswith('.wav'):
                filename_elements = file.split('_')
                dictor_dir = filename_elements[0]
                text_type = filename_elements[1]
                dictor_name = filename_elements[2]

                tail = filename_elements[-1]
                if re.findall('\d', tail):
                    status = ''
                    cut_num = int(tail.split('.')[0])
                    indxs = filename_elements[-2]
                else:
                    status = tail.split('.')[0]
                    cut_num = int(filename_elements[-2])
                    indxs = filename_elements[-3]
                st_idx = int(indxs.split('-')[0])
                txt_idx = cut_num - 1
                new_idx = st_idx + txt_idx

                csvfilename = f'{dictor_dir}/{text_type}/{dictor_name}_{new_idx}.wav'
                if marked_texts[txt_idx] != \
                        marker_df.loc[marker_df['file_name'] == csvfilename, 'original_text'].tolist()[0].replace(
                            f'{new_idx} ', ''):
                    marker_df.loc[marker_df['file_name'] == csvfilename, 'marked_text'] = marked_texts[txt_idx]
                    if status:
                        status += '_corrected'
                    else:
                        status += 'corrected'
                if status:
                    ydfilename = f'{dictor_dir}/{text_type}/{dictor_name}_{new_idx}_{status}.wav'
                else:
                    ydfilename = f'{dictor_dir}/{text_type}/{dictor_name}_{new_idx}.wav'

                logging(message, file)
                out_str += upload_to_yd(project_name, os.path.join(files_path, file), ydfilename)[0]
                logging(message, f'{ydfilename} загружен успешно')

                marker_df.loc[marker_df['file_name'] == csvfilename, 'done_date'] = upload_date
                marker_df.loc[marker_df['file_name'] == csvfilename, 'status'] = 'done'
                marker_df.loc[marker_df['file_name'] == csvfilename, 'file_name'] = ydfilename

        marker_df.to_csv(MARKERS_SOUND_CSV, index=False)
        timetable = gc.open(DICTORS_TABLE_NAME)
        time_worksheet = timetable.worksheet(DICTORS_WORKSHEET_NAME)
        set_with_dataframe(time_worksheet, marker_df)
    return out_str
