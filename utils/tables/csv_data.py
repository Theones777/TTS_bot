import datetime
import os
import re
import shutil
import warnings
import zipfile

import pandas as pd
from gspread_dataframe import set_with_dataframe
from pymediainfo import MediaInfo

from utils.log import logging
from utils.tables.google_sheets import gc
from utils.variables import LONGS_TABLE_NAME, METKI_PATH, TMP_ARC_PATH, AVAIL_AUDIO_PROJECTS_NAMES, LONG_AUDIOS_CSV, \
    TMP_DOWNLOAD_PATH, IDX_FILENAME_COL, CURATOR_HOMOGRAPH_CSV, USER_BACKUP_PATH, \
    YD_DONE_NOT_ACCEPTED_HOMOGRAPHS_PATH, CURATOR_YOMOGRAPH_CSV, YD_DONE_NOT_ACCEPTED_YOMOGRAPHS_PATH, \
    DICTORS_TEXTS_PATH, \
    YD_ROOT_DICTORS_DONE_AUDIOS_PATH, DICTORS_TABLE_NAME, YD_DONE_ACCEPTED_HOMOGRAPHS_PATH, \
    YD_DONE_ACCEPTED_YOMOGRAPHS_PATH, AVAIL_CURATORS_PROJECTS
from utils.yd_dir.yd_download import simple_download, download_audio_files_from_yd
from utils.yd_dir.yd_init import y_disk
from utils.yd_dir.yd_upload import upload_to_yd

warnings.simplefilter(action='ignore', category=FutureWarning)


def get_specific_word(message, curator_id, project_name):
    word = f'_{message.text}_'
    if project_name == AVAIL_CURATORS_PROJECTS[0]:
        csv_path = CURATOR_HOMOGRAPH_CSV
        dones_path = YD_DONE_ACCEPTED_HOMOGRAPHS_PATH
        dones_path2 = YD_DONE_NOT_ACCEPTED_HOMOGRAPHS_PATH
    else:
        csv_path = CURATOR_YOMOGRAPH_CSV
        dones_path = YD_DONE_ACCEPTED_YOMOGRAPHS_PATH
        dones_path2 = YD_DONE_NOT_ACCEPTED_YOMOGRAPHS_PATH

    df = pd.read_csv(csv_path)
    if len(df.loc[df['file_name'].str.contains(word.replace('+', '\+')), 'file_name'].tolist()) == 0:
        for f in y_disk.listdir(dones_path2):
            if f.name not in df['file_name'].tolist():
                df = df.append({'file_name': f.name,
                                'upload_date': f.created,
                                'curator_id': 'none'}, ignore_index=True)
        for f in y_disk.listdir(dones_path):
            if f.name not in df['file_name'].tolist():
                df = df.append({'file_name': f.name,
                                'upload_date': f.created,
                                'curator_id': 'none'}, ignore_index=True)
        try:
            df.sort_values('upload_date', ascending=True, inplace=True, ignore_index=True)
        except:
            pass
        df.to_csv(csv_path, index=False)
    file_name = df.loc[df['file_name'].str.contains(word.replace('+', '\+')), 'file_name'].tolist()[0]
    logging(message, file_name)
    try:
        simple_download(f'{dones_path}/{file_name}', os.path.join(TMP_ARC_PATH, file_name))
    except:
        simple_download(f'{dones_path2}/{file_name}', os.path.join(TMP_ARC_PATH, file_name))
    df.loc[df['file_name'] == file_name, 'curator_id'] = curator_id
    df.to_csv(csv_path, index=False)

    return os.path.join(TMP_ARC_PATH, file_name)


def get_curator_words(message, curator_id, project_name):
    words_num = int(message.text)
    today = datetime.datetime.today().isoformat(sep=" ").split(' ')[0]
    arc_path = os.path.join(TMP_ARC_PATH, f'{today}_{curator_id}.zip')
    if project_name == AVAIL_CURATORS_PROJECTS[0]:
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

    if project_name == AVAIL_AUDIO_PROJECTS_NAMES[0]:
        long_df = pd.read_csv(LONG_AUDIOS_CSV)
        tmp_long_df = long_df[long_df['status'] == 'waiting']
        wav_path = tmp_long_df.iloc[0, IDX_FILENAME_COL]
        wav_path = wav_path.replace('Tatyana', 'tatyana')
        long_df.loc[long_df['file_name'] == wav_path, 'status'] = 'coping'
        long_df.to_csv(LONG_AUDIOS_CSV, index=False)
        os.makedirs(os.path.join(TMP_DOWNLOAD_PATH, user_id), exist_ok=True)

        dictor_name = wav_path.split('/')[0]
        text_type = wav_path.split('/')[1]
        wav_name = wav_path.split('/')[2]
        logging(message, wav_path)
        download_audio_files_from_yd(wav_path, user_id, dictor_name, text_type, wav_name)
        logging(message, 'Загрузка успешна')
        arc_path = os.path.join(TMP_ARC_PATH, f'{wav_name}_{user_id}.zip')

        with zipfile.ZipFile(arc_path, 'w') as myzip:
            myzip.write(os.path.join(TMP_DOWNLOAD_PATH, user_id, dictor_name, text_type, wav_name),
                        arcname=f'{dictor_name}_{text_type}_{wav_name}')

            with open(os.path.join(DICTORS_TEXTS_PATH, f'{text_type}.txt'), encoding='utf-8') as f:
                common_text = f.readlines()
            if text_type in ['corpus', 'questions']:
                start_idx = int(wav_name.split('_')[1].split('-')[0]) - 1
                end_idx = int(wav_name.split('_')[1].split('-')[1].split('.')[0])
                r_st = start_idx + 1
                r_end = end_idx + 1
            else:
                start_idx = int(wav_name.split('_')[1].split('-')[0])
                end_idx = int(wav_name.split('_')[1].split('-')[1].split('.')[0]) + 1
                r_st = start_idx
                r_end = end_idx

            text = common_text[start_idx:end_idx]
            txt_name = os.path.join(TMP_DOWNLOAD_PATH, user_id, dictor_name, text_type,
                                    wav_name.replace('.wav', '.txt'))
            with open(txt_name, 'w', encoding='utf-8') as f:
                f.writelines(text)
            myzip.write(txt_name, arcname=f"{dictor_name}_{text_type}_{wav_name.replace('.wav', '.txt')}")
            metki_name = os.path.join(METKI_PATH, wav_name.replace('.wav', '_mt.txt'))
            myzip.write(metki_name, arcname=f"{dictor_name}_{text_type}_{wav_name.replace('.wav', '_mt.txt')}")
        
        for text_i, i in enumerate(range(r_st, r_end)):
            marker_df = pd.read_csv(f'utils_data/csvs/{dictor_name}.csv')
            new_wav_name = f'{wav_name.split("_")[0]}_{i}.wav'
            marker_df = marker_df.append({'file_name': f'{dictor_name}/{text_type}/{new_wav_name}',
                                          'status': 'in_process',
                                          'marker_id': user_id,
                                          'original_text': text[text_i],
                                          'curator_status': 'in_process'}, ignore_index=True)
            marker_df.to_csv(f'utils_data/csvs/{dictor_name}.csv', index=False)

        long_df.loc[long_df['file_name'] == wav_path, 'status'] = 'in_process'
        long_df.loc[long_df['file_name'] == wav_path, 'marker_id'] = user_id
        long_df.to_csv(LONG_AUDIOS_CSV, index=False)
        
        shutil.rmtree(TMP_DOWNLOAD_PATH)
        try:
            timetable = gc.open(DICTORS_TABLE_NAME)
            time_worksheet = timetable.worksheet(dictor_name.strip())
            set_with_dataframe(time_worksheet, marker_df)

            timetable = gc.open(LONGS_TABLE_NAME)
            time_worksheet = timetable.worksheet('audios')
            set_with_dataframe(time_worksheet, long_df)
        except:
            pass
    return arc_path


def enter_audio_data(message, user_id, project_name, flag, file_path=''):
    out_str = ''
    if project_name == AVAIL_AUDIO_PROJECTS_NAMES[0]:
        files_path = os.path.join(TMP_DOWNLOAD_PATH, user_id, 'unzip_files')
        today = datetime.datetime.today().isoformat(sep=" ").split(' ')[0]
        upload_date = '.'.join([today.split('-')[2], today.split('-')[1], today.split('-')[0]])

        if flag == 'marker':
            txt_file = [f for f in os.listdir(files_path) if f.endswith('.txt')][0]
            rpp_file = [f for f in os.listdir(files_path) if not f.endswith('.txt')][0]
            dictor_name = txt_file.split('_')[0].strip()
            text_type = txt_file.split('_')[1]

            long_df = pd.read_csv(LONG_AUDIOS_CSV)
            long_df.loc[
                long_df[
                    'file_name'] == f'{dictor_name}/{text_type}/{"_".join(txt_file.split("_")[2:]).replace(".txt", ".wav")}',
                'status'] = 'done'
            long_df.to_csv(LONG_AUDIOS_CSV, index=False)
            try:
                timetable = gc.open(LONGS_TABLE_NAME)
                time_worksheet = timetable.worksheet('audios')
                set_with_dataframe(time_worksheet, long_df)
            except:
                pass
            with open(os.path.join(files_path, txt_file), encoding='utf-8') as f:
                tmp_texts = f.readlines()
                marked_texts = [el for el in tmp_texts if el != '\n']
            shutil.copy(os.path.join(files_path, txt_file), os.path.join(USER_BACKUP_PATH, txt_file))
            for string in marked_texts:
                marker_df = pd.read_csv(f'utils_data/csvs/{dictor_name}.csv')
                new_idx = int(''.join(re.findall('\d', string[:8])))
                csv_filename = f'{dictor_name}/{text_type}/{dictor_name}_{new_idx}.wav'
                try:
                    if string != marker_df.loc[marker_df['file_name'] == csv_filename, 'original_text'].tolist()[0]:
                        marker_df.loc[marker_df['file_name'] == csv_filename, 'marked_text'] = string
                except:
                    pass

                logging(message, txt_file)

                marker_df.loc[marker_df['file_name'] == csv_filename, 'done_date'] = upload_date
                marker_df.loc[marker_df['file_name'] == csv_filename, 'status'] = 'done'
                marker_df.loc[marker_df['file_name'] == csv_filename, 'curator_status'] = 'На проверке'

                marker_df.to_csv(f'utils_data/csvs/{dictor_name}.csv', index=False)
            
            yd_rpp_name = '_'.join(rpp_file.split('_')[2:]).replace('.RPP', '.rpp')
            yd_filename = f'{dictor_name}/{text_type}/{yd_rpp_name}'
            out_str += upload_to_yd('marker_audio_project', os.path.join(files_path, rpp_file), yd_filename)[0]
            
            timetable = gc.open(DICTORS_TABLE_NAME)
            time_worksheet = timetable.worksheet(dictor_name)
            set_with_dataframe(time_worksheet, marker_df)

            if not out_str:
                out_str = 'Файл с проектом загружен'
        else:
            # os.makedirs(files_path, exist_ok=True)
            # with zipfile.ZipFile(file_path) as zf:
            #     zf.extractall(files_path)

            filename = file_path.split(os.sep)[-1]
            dictor_name = filename.split('_')[0].strip()
            text_type = filename.split('_')[1]
            # st_idx = int(filename.split('-')[0].split('_')[-1])
            st_idx = int(filename.split('_')[-1].split('-')[0])

            dictors_dirs = [dd.name for dd in y_disk.listdir(YD_ROOT_DICTORS_DONE_AUDIOS_PATH)]
            if dictor_name not in dictors_dirs:
                y_disk.mkdir(f'{YD_ROOT_DICTORS_DONE_AUDIOS_PATH}/{dictor_name}')
            text_type_dirs = [ttd.name for ttd in y_disk.listdir(f'{YD_ROOT_DICTORS_DONE_AUDIOS_PATH}/{dictor_name}')]
            if text_type not in text_type_dirs:
                y_disk.mkdir(f'{YD_ROOT_DICTORS_DONE_AUDIOS_PATH}/{dictor_name}/{text_type}')
            
            with open(file_path) as f:
                metki = f.readlines()
            
            for i, row in enumerate(metki):
                cut_num = i+st_idx
                els = row.split('\t')
                tmp_cut_status = els[-1].split()
                if tmp_cut_status:
                    cut_status = tmp_cut_status[0].strip()
                else:
                    cut_status = ''

                if cut_status in ['defect', 'misc', 'pause']:
                    status = cut_status
                else:
                    status = ''
                
                marker_df = pd.read_csv(f'utils_data/csvs/{dictor_name}.csv')
                csv_filename = f'{dictor_name}/{text_type}/{dictor_name}_{cut_num}.wav'
                csv_filename_corr = f'{dictor_name}/{text_type}/{dictor_name}_{cut_num}_corrected.wav'
                try:
                    if type(marker_df.loc[marker_df['file_name'] == csv_filename, 'marked_text'].tolist()[0]) != float:
                        if status:
                            status += '_corrected'
                        else:
                            status += 'corrected'
                except:
                    pass

                if status:
                    ydfilename = f'{dictor_name}/{text_type}/{dictor_name}_{cut_num}_{status}.wav'
                else:
                    ydfilename = f'{dictor_name}/{text_type}/{dictor_name}_{cut_num}.wav'

                logging(message, csv_filename)
                logging(message, f'{ydfilename} загружен успешно')
                
                marker_df.loc[marker_df['file_name'] == csv_filename, 'curator_status'] = 'Проверено'
                marker_df.loc[marker_df['file_name'] == csv_filename_corr, 'curator_status'] = 'Проверено'
                marker_df.loc[marker_df['file_name'] == csv_filename, 'file_name'] = ydfilename
                marker_df.to_csv(f'utils_data/csvs/{dictor_name}.csv', index=False)

            out_str += upload_to_yd(project_name, file_path, filename)[0]



            # for file in os.listdir(files_path):
            #     marker_df = pd.read_csv(f'utils_data/csvs/{dictor_name}.csv')
            #     filename_elements = file.split('_')
            #     cut_num = int(file.split('-')[-1].split('.')[0])

            #     mi = MediaInfo.parse(os.path.join(files_path, file))
            #     status = mi.tracks[0].title if mi.tracks[0].title in ['defect', 'pause', 'misc'] else ''

                # if re.findall('\d', filename_elements[3]):
                #     status = ''
                # else:
                #     status = filename_elements[3]

                # st_idx = int(filename_elements[-1].split('-')[0])
                # txt_idx = cut_num - 1
                # new_idx = st_idx + txt_idx

            #     csv_filename = f'{dictor_name}/{text_type}/{dictor_name}_{new_idx}.wav'
            #     csv_filename_corr = f'{dictor_name}/{text_type}/{dictor_name}_{new_idx}_corrected.wav'
            #     try:
            #         if type(marker_df.loc[marker_df['file_name'] == csv_filename, 'marked_text'].tolist()[0]) != float:
            #             if status:
            #                 status += '_corrected'
            #             else:
            #                 status += 'corrected'
            #     except:
            #         pass

            #     if status:
            #         ydfilename = f'{dictor_name}/{text_type}/{dictor_name}_{new_idx}_{status}.wav'
            #     else:
            #         ydfilename = f'{dictor_name}/{text_type}/{dictor_name}_{new_idx}.wav'

            #     logging(message, file)
            #     out_str += upload_to_yd(project_name, os.path.join(files_path, file), ydfilename)[0]
            #     logging(message, f'{ydfilename} загружен успешно')
                
            #     marker_df.loc[marker_df['file_name'] == csv_filename, 'curator_status'] = 'Проверено'
            #     marker_df.loc[marker_df['file_name'] == csv_filename_corr, 'curator_status'] = 'Проверено'
            #     marker_df.loc[marker_df['file_name'] == csv_filename, 'file_name'] = ydfilename
            #     marker_df.to_csv(f'utils_data/csvs/{dictor_name}.csv', index=False)

            timetable = gc.open(DICTORS_TABLE_NAME)
            time_worksheet = timetable.worksheet(dictor_name)
            set_with_dataframe(time_worksheet, marker_df)
            if not out_str:
                out_str = 'Все файлы загружены'
    return out_str


def get_long_audios_names(dictor_name, text_type):
    long_df = pd.read_csv(LONG_AUDIOS_CSV)
    all_long_audios_names = long_df[long_df['status']=='done']['file_name'].tolist()
    long_audios_names = [name for name in all_long_audios_names if dictor_name in name and text_type in name]
    return long_audios_names


def insert_curator_id(long_audio_name, curator_id):
    long_df = pd.read_csv(LONG_AUDIOS_CSV)
    long_df.loc[long_df['file_name'] == long_audio_name, 'status'] = curator_id
    long_df.to_csv(LONG_AUDIOS_CSV, index=False)


def get_audio_for_text_check(curator_id):
    long_df = pd.read_csv(LONG_AUDIOS_CSV)
    tmp_df = long_df[long_df['text_check']=='waiting']
    file_name = tmp_df.iloc[0, 0]
    dictor = file_name.split('/')[0]
    text_type = file_name.split('/')[1]

    long_df.loc[long_df['file_name'] == file_name, 'text_check'] = curator_id
    long_df.to_csv(LONG_AUDIOS_CSV, index=False)

    i1 = int(file_name.split('_')[-1].split('-')[0])
    i2 = int(file_name.split('_')[-1].split('-')[-1].split('.')[0])
    arc_path = os.path.join(TMP_ARC_PATH, f'{file_name}_{curator_id}.zip')

    with zipfile.ZipFile(arc_path, 'w') as myzip:
        for i in range(i2 - i1 + 1):
            try:
                sample_name = f'{dictor}_{i1+i}.wav'
                simple_download(f'/cooperation/dictors/marked/{dictor}/{text_type}/{sample_name}',
                                os.path.join(TMP_ARC_PATH, f'{dictor}_{text_type}_{sample_name}'))
            except:
                try:
                    sample_name = f'{dictor}_{i1 + i}_corrected.wav'
                    simple_download(f'/cooperation/dictors/marked/{dictor}/{text_type}/{sample_name}',
                                    os.path.join(TMP_ARC_PATH, f'{dictor}_{text_type}_{sample_name}'))
                except:
                    pass

            myzip.write(os.path.join(TMP_ARC_PATH, f'{dictor}_{text_type}_{sample_name}'),
                        arcname=f'{dictor}_{text_type}_{sample_name}')
            os.remove(os.path.join(TMP_ARC_PATH, f'{dictor}_{text_type}_{sample_name}'))

    return arc_path
