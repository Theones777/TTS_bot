import datetime
import os
import shutil
import zipfile

import pandas as pd

from utils.log import logging
from utils.variables import TMP_ARC_PATH, AVAIL_AUDIO_PROJECTS_NAMES, LONG_AUDIOS_CSV, YD_ROOT_DICTORS_RAW_AUDIOS_PATH, \
    TMP_DOWNLOAD_PATH, IDX_FILENAME_COL, AVAIL_TXT_PROJECTS_NAMES, CURATOR_HOMOGRAPH_CSV, \
    YD_DONE_NOT_ACCEPTED_HOMOGRAPHS_PATH, CURATOR_YOMOGRAPH_CSV, YD_DONE_NOT_ACCEPTED_YOMOGRAPHS_PATH, \
    YD_ROOT_DICTORS_CUTTED_AUDIOS_PATH, SOUND_MEN_PROFILES, MARKERS_SOUND_CSV, DICTORS_TEXTS_PATH, \
    YD_ROOT_DICTORS_DONE_AUDIOS_PATH
from utils.yd_dir.yd_download import simple_download, download_audio_files_from_yd
from utils.yd_dir.yd_init import y_disk
from utils.yd_dir.yd_upload import upload_to_yd


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
    examples_num = 1
    today = datetime.datetime.today().isoformat(sep=" ").split(' ')[0]
    arc_path = os.path.join(TMP_ARC_PATH, f'{today}_{user_id}.zip')

    if project_name == AVAIL_AUDIO_PROJECTS_NAMES[0]:
        df_path = LONG_AUDIOS_CSV
        root_yd_path = YD_ROOT_DICTORS_RAW_AUDIOS_PATH
        id_col = 'marker_id'

        df = pd.read_csv(df_path)
        tmp_df = df[df['status'] == 'waiting']
        if len(tmp_df) < examples_num:
            for doctor_name in y_disk.listdir(root_yd_path):
                for text_type in y_disk.listdir(f'{root_yd_path}/{doctor_name.name}'):
                    for filename in y_disk.listdir(f'{root_yd_path}/{doctor_name.name}/{text_type.name}'):
                        insert_value = f'{doctor_name.name}/{text_type.name}/{filename.name}'
                        if insert_value not in df['audio_path'].tolist():
                            if marker_flag:
                                str_idx = int(filename.name.split('_')[1].split('.')[0]) - 1
                                with open(os.path.join(DICTORS_TEXTS_PATH, f'{text_type.name}.txt'),
                                          encoding='utf-8') as f:
                                    common_text = f.readlines()
                                orig_text = common_text[str_idx]
                                df = df.append({'audio_path': insert_value,
                                                'status': 'waiting',
                                                'original_text': orig_text}, ignore_index=True)
                            else:
                                df = df.append({'audio_path': insert_value,
                                                'status': 'waiting'}, ignore_index=True)
            df.to_csv(df_path, index=False)
            tmp_df = df[df['status'] == 'waiting']

        output_files_paths = tmp_df.iloc[0:examples_num, IDX_FILENAME_COL].tolist()
        for wav_path in output_files_paths:
            df.loc[df['audio_path'] == wav_path, 'status'] = 'coping'
        df.to_csv(df_path, index=False)

        os.makedirs(os.path.join(TMP_DOWNLOAD_PATH, user_id), exist_ok=True)
        with zipfile.ZipFile(arc_path, 'w') as myzip:
            for wav_path in output_files_paths:
                dictor_name = wav_path.split('/')[0]
                text_type = wav_path.split('/')[1]
                wav_name = wav_path.split('/')[2]
                logging(message, wav_path)
                download_audio_files_from_yd(wav_path, user_id, dictor_name, text_type, wav_name)
                logging(message, 'Загрузка успешна')
                if not marker_flag:
                    myzip.write(os.path.join(TMP_DOWNLOAD_PATH, user_id, dictor_name, text_type, wav_name),
                                arcname=os.path.join(dictor_name, text_type, wav_name))
                    start_idx = int(wav_name.split('_')[1].split('-')[0]) - 1
                    end_idx = int(wav_name.split('_')[1].split('-')[1].split('.')[0])
                    with open(os.path.join(DICTORS_TEXTS_PATH, f'{text_type}.txt'), encoding='utf-8') as f:
                        common_text = f.readlines()
                        text = common_text[start_idx:end_idx]

                    txt_name = os.path.join(TMP_DOWNLOAD_PATH, user_id, dictor_name, text_type,
                                            wav_name.replace('.wav', '.txt'))
                    with open(txt_name, 'w', encoding='utf-8') as f:
                        f.writelines(text)
                    myzip.write(txt_name,
                                arcname=os.path.join(dictor_name, text_type, wav_name.replace('.wav', '.txt')))
                else:
                    idx = wav_name.split('_')[1].split('.')[0]
                    myzip.write(os.path.join(TMP_DOWNLOAD_PATH, user_id, dictor_name, text_type, wav_name),
                                arcname=f'{dictor_name}_{text_type}_{wav_name}')
                    txt_name = os.path.join(TMP_DOWNLOAD_PATH, user_id, dictor_name, text_type,
                                            wav_name.replace('.wav', '.txt'))
                    with open(txt_name, 'w', encoding='utf-8') as f:
                        text = df.loc[df['audio_path'] == wav_path, 'original_text'].tolist()[0]
                        f.write(text.replace(idx, '').strip())
                    myzip.write(txt_name, arcname=f"{dictor_name}_{text_type}_{wav_name.replace('.wav', '.txt')}")

                df.loc[df['audio_path'] == wav_path, 'status'] = 'in_process'
                df.loc[df['audio_path'] == wav_path, id_col] = user_id
        df.to_csv(df_path, index=False)
        shutil.rmtree(TMP_DOWNLOAD_PATH)
    return arc_path


def enter_audio_data(message, user_id, project_name, add_component):
    out_str = ''
    if project_name == AVAIL_AUDIO_PROJECTS_NAMES[0]:
        files_path = os.path.join(TMP_DOWNLOAD_PATH, user_id, 'unzip_files')
        os.makedirs(files_path, exist_ok=True)
        if type(add_component) == str:
            with zipfile.ZipFile(add_component) as zf:
                zf.extractall(files_path)
            flag = 'sound_man'
            csv_path = LONG_AUDIOS_CSV
            yd_root_path = YD_ROOT_DICTORS_CUTTED_AUDIOS_PATH
        else:
            flag = 'marker'
            csv_path = MARKERS_SOUND_CSV
            yd_root_path = YD_ROOT_DICTORS_DONE_AUDIOS_PATH

        df = pd.read_csv(csv_path)
        if flag == 'sound_man':
            for dictor_name in os.listdir(files_path):
                dictors_dirs = [dd.name for dd in y_disk.listdir(yd_root_path)]
                if dictor_name not in dictors_dirs:
                    y_disk.mkdir(f'{yd_root_path}/{dictor_name}')
                for text_type in os.listdir(os.path.join(files_path, dictor_name)):
                    text_type_dirs = [ttd.name for ttd in y_disk.listdir(f'{yd_root_path}/{dictor_name}')]
                    if text_type not in text_type_dirs:
                        y_disk.mkdir(f'{yd_root_path}/{dictor_name}/{text_type}')
                    for filename in os.listdir(os.path.join(files_path, dictor_name, text_type)):
                        if filename.endswith('.wav'):
                            logging(message, filename)
                            out_str += upload_to_yd(project_name,
                                                    os.path.join(files_path, dictor_name, text_type, filename),
                                                    f'{dictor_name}/{text_type}/{filename}', flag)[0]
                            logging(message, f'{filename} загружен успешно')
                        else:
                            df.loc[df['audio_path'] == f'{dictor_name}/{text_type}/{filename.replace(".txt", ".wav")}',
                                   'status'] = 'done'

        else:
            today = datetime.datetime.today().isoformat(sep=" ").split(' ')[0]
            upload_date = '.'.join([today.split('-')[2], today.split('-')[1], today.split('-')[0]])
            dictors = set([fl.split('_')[0] for fl in os.listdir(files_path)])
            text_types = set([fl.split('_')[1] for fl in os.listdir(files_path)])
            dictors_dirs = [dd.name for dd in y_disk.listdir(yd_root_path)]
            for dictor_name in dictors:
                if dictor_name not in dictors_dirs:
                    y_disk.mkdir(f'{yd_root_path}/{dictor_name}')
                text_type_dirs = [ttd.name for ttd in y_disk.listdir(f'{yd_root_path}/{dictor_name}')]
                for text_type in text_types:
                    if text_type not in text_type_dirs:
                        y_disk.mkdir(f'{yd_root_path}/{dictor_name}/{text_type}')

            for fl in [f for f in os.listdir(files_path) if f.endswith('.wav')]:
                dictor_name = fl.split('_')[0]
                text_type = fl.split('_')[1]
                filename = '_'.join(fl.split('_')[2:])

                upload_result = upload_to_yd(project_name, os.path.join(files_path, fl),
                                             f'{dictor_name}/{text_type}/{filename}', flag)
                out_str += upload_result[0]

                if upload_result[1]:
                    out_str += upload_to_yd(project_name, os.path.join(files_path, fl.replace('.wav', '.txt')),
                                            f"{dictor_name}/{text_type}/{filename.replace('.wav', '.txt')}", flag)[0]
                    with open(os.path.join(files_path, fl.replace('.wav', '.txt')), encoding='utf-8') as f:
                        marked_text = f.read().strip()

                    df.loc[df['audio_path'] == f'{dictor_name}/{text_type}/{filename}', 'done_date'] = upload_date
                    df.loc[df['audio_path'] == f'{dictor_name}/{text_type}/{filename}', 'marked_text'] = marked_text
                    df.loc[df['audio_path'] == f'{dictor_name}/{text_type}/{filename}', 'status'] = 'done'

        df.to_csv(csv_path, index=False)
    return out_str
