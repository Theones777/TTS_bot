import datetime
import os
import time
import zipfile

import gspread

from utils.log import logging
from utils.variables import HOMO_TABLE_NAME, GOOGLE_SHEET_JSON_PATH, TMP_ARC_PATH, \
    AVAIL_TXT_PROJECTS_NAMES, HOMOGRAPHS_COORDINATES, YOMO_TABLE_NAME, YOMOGRAPHS_COORDINATES, CURATORS_TABLE_NAME, \
    CURATORS_COORDINATES, MARKERS_NAMES_AND_TIMETABLES, TIMETABLE_LIST, TIMETABLE_MISTAKES_COL, AVAIL_CURATORS_PROJECTS
from utils.yd_dir.yd_download import yd_doc_download
from utils.yd_dir.yd_upload import move_file_to_yd

gc = gspread.service_account(filename=GOOGLE_SHEET_JSON_PATH)


def insert_to_timetable(insert_info: int, raw_date: str, marker_id: str):
    timetable = gc.open(MARKERS_NAMES_AND_TIMETABLES[marker_id][1])
    time_worksheet = timetable.worksheet(TIMETABLE_LIST)
    if len(raw_date.split('.')[0]) == 1:
        raw_date = '0' + raw_date
    if len(raw_date.split('.')[1]) == 1:
        raw_date = '.'.join([raw_date.split('.')[0], '0' + raw_date.split('.')[1], raw_date.split('.')[2]])
    date = raw_date[0:-4] + raw_date[-2:]
    time_cell = time_worksheet.find(date)
    already_done_value = time_worksheet.cell(time_cell.row, TIMETABLE_MISTAKES_COL).value
    if already_done_value:
        insert_info += int(already_done_value)
    time_worksheet.update_cell(time_cell.row, TIMETABLE_MISTAKES_COL, insert_info)


def insert_curator_info(info_to_insert, handle_mistakes_num, upload_date, page_marker_id):
    sh = gc.open(CURATORS_TABLE_NAME)
    worksheet = sh.worksheet(page_marker_id)
    if len(upload_date.split('.')[0]) == 1:
        upload_date = '0' + upload_date
    if len(upload_date.split('.')[1]) == 1:
        upload_date = '.'.join([upload_date.split('.')[0], '0' + upload_date.split('.')[1], upload_date.split('.')[2]])
    cell = worksheet.find(upload_date)
    mistakes_num_col = CURATORS_COORDINATES['handle_num_mistakes_col']
    mistakes_col = CURATORS_COORDINATES['handle_mistakes_col']

    while worksheet.cell(cell.row, mistakes_num_col).value:
        time.sleep(1)
        mistakes_num_col += 2
        mistakes_col += 2
    worksheet.update_cell(cell.row, mistakes_num_col, handle_mistakes_num)
    worksheet.update_cell(cell.row, mistakes_col, info_to_insert)


def get_marker_id(word, project_name):
    row = 0
    if project_name == AVAIL_CURATORS_PROJECTS[0]:
        sh = gc.open(HOMO_TABLE_NAME)
        coordinates_table = HOMOGRAPHS_COORDINATES
    else:
        sh = gc.open(YOMO_TABLE_NAME)
        coordinates_table = YOMOGRAPHS_COORDINATES

    worksheet = sh.get_worksheet(0)
    time.sleep(1)
    for tmp_cell in worksheet.findall(word):
        if tmp_cell.col == coordinates_table['main_words_col']:
            row = tmp_cell.row
            break

    marker_id = worksheet.cell(row, coordinates_table['marker_id']).value
    add_markup_marker_id = worksheet.cell(row, coordinates_table['add_markup_marker_id']).value
    if 'done' in worksheet.cell(row, coordinates_table['status']).value:
        status = 'done'
    else:
        status = 'hard'
    return marker_id, add_markup_marker_id, status


def get_text_markup_data(message, marker_id, project_name):
    words_num = int(message.text)
    today = datetime.datetime.today().isoformat(sep=" ").split(' ')[0]
    arc_path = os.path.join(TMP_ARC_PATH, f'{today}_{marker_id}.zip')
    if project_name == AVAIL_TXT_PROJECTS_NAMES[0]:
        sh = gc.open(HOMO_TABLE_NAME)
        worksheet = sh.get_worksheet(0)
        time.sleep(1)
        with zipfile.ZipFile(arc_path, 'w') as myzip:
            for i in range(words_num):
                try:
                    waiting_cells = worksheet.findall("waiting")
                    cell = waiting_cells[0]
                    word = worksheet.cell(cell.row, HOMOGRAPHS_COORDINATES['main_words_col']).value
                    words_done_count = int(
                        worksheet.cell(cell.row, HOMOGRAPHS_COORDINATES['word_done_count_col']).value)
                    logging(message, word)
                    avail_words = int(
                        worksheet.cell(cell.row, HOMOGRAPHS_COORDINATES['available_col']).value)
                    word_need_count = words_done_count - avail_words
                    with open(os.path.join(TMP_ARC_PATH, f'{word}_{word_need_count}.txt'), 'w') as f:
                        f.write('')
                    worksheet.update_cell(cell.row, HOMOGRAPHS_COORDINATES['marker_id'], marker_id)
                    myzip.write(os.path.join(TMP_ARC_PATH, f'{word}_{word_need_count}.txt'),
                                arcname=f'{word}_{word_need_count}.txt')
                    os.remove(os.path.join(TMP_ARC_PATH, f'{word}_{word_need_count}.txt'))
                except:
                    pass

    elif project_name == AVAIL_TXT_PROJECTS_NAMES[1]:
        sh = gc.open(HOMO_TABLE_NAME)
        worksheet = sh.get_worksheet(0)
        time.sleep(1)
        start_row_num = 3
        with zipfile.ZipFile(arc_path, 'w') as myzip:
            for i in range(words_num):
                try:
                    cell = worksheet.cell(start_row_num, HOMOGRAPHS_COORDINATES['add_markup_marker_id_check'])
                    while cell.value:
                        time.sleep(1)
                        start_row_num += 1
                        cell = worksheet.cell(start_row_num, HOMOGRAPHS_COORDINATES['add_markup_marker_id_check'])

                    word = worksheet.cell(cell.row, HOMOGRAPHS_COORDINATES['add_markup_word_check']).value

                    logging(message, word)
                    avail_words = int(
                        worksheet.cell(cell.row, HOMOGRAPHS_COORDINATES['add_markup_sum_col']).value)
                    if worksheet.cell(cell.row, HOMOGRAPHS_COORDINATES['add_markup_add_col']).value:
                        add_words = int(worksheet.cell(cell.row, HOMOGRAPHS_COORDINATES['add_markup_add_col']).value)
                    else:
                        add_words = 0

                    for tmp_cell in worksheet.findall(word):
                        if tmp_cell.col == HOMOGRAPHS_COORDINATES['main_words_col']:
                            upd_raw = tmp_cell.row
                            prev_marker_id = worksheet.cell(upd_raw, HOMOGRAPHS_COORDINATES['marker_id']).value

                    words_done_count = int(
                        worksheet.cell(upd_raw, HOMOGRAPHS_COORDINATES['word_done_count_col']).value)
                    word_need_count = words_done_count - avail_words
                    worksheet.update_cell(upd_raw, HOMOGRAPHS_COORDINATES['add_markup_marker_id'], marker_id)

                    arcname = f'{word}_{word_need_count}.txt'
                    if add_words:
                        yd_doc_download(message, word, word_need_count, prev_marker_id, project_name)
                        time.sleep(1)
                    else:
                        with open(os.path.join(TMP_ARC_PATH, arcname), 'w') as f:
                            f.write('')

                    myzip.write(os.path.join(TMP_ARC_PATH, arcname), arcname=arcname)
                    os.remove(os.path.join(TMP_ARC_PATH, f'{word}_{word_need_count}.txt'))
                except:
                    pass

    elif project_name == AVAIL_TXT_PROJECTS_NAMES[2]:
        sh = gc.open(YOMO_TABLE_NAME)
        worksheet = sh.get_worksheet(0)
        time.sleep(1)
        start_row_num = 3
        with zipfile.ZipFile(arc_path, 'w') as myzip:
            for i in range(words_num):
                try:
                    cell = worksheet.cell(start_row_num, YOMOGRAPHS_COORDINATES['add_markup_marker_id_check'])
                    while cell.value:
                        time.sleep(1)
                        start_row_num += 1
                        cell = worksheet.cell(start_row_num, YOMOGRAPHS_COORDINATES['add_markup_marker_id_check'])

                    word = worksheet.cell(cell.row, YOMOGRAPHS_COORDINATES['add_markup_word_check']).value

                    logging(message, word)
                    avail_words = int(
                        worksheet.cell(cell.row, YOMOGRAPHS_COORDINATES['add_markup_sum_col']).value)
                    if worksheet.cell(cell.row, YOMOGRAPHS_COORDINATES['add_markup_add_col']).value:
                        add_words = int(worksheet.cell(cell.row, YOMOGRAPHS_COORDINATES['add_markup_add_col']).value)
                    else:
                        add_words = 0

                    for tmp_cell in worksheet.findall(word):
                        if tmp_cell.col == YOMOGRAPHS_COORDINATES['main_words_col']:
                            upd_raw = tmp_cell.row
                            prev_marker_id = worksheet.cell(upd_raw, YOMOGRAPHS_COORDINATES['marker_id']).value

                    words_done_count = int(
                        worksheet.cell(upd_raw, YOMOGRAPHS_COORDINATES['word_done_count_col']).value)
                    word_need_count = words_done_count - avail_words

                    worksheet.update_cell(upd_raw, YOMOGRAPHS_COORDINATES['add_markup_marker_id'], marker_id)

                    arcname = f'{word}_{word_need_count}.txt'
                    if add_words:
                        yd_doc_download(message, word, word_need_count, prev_marker_id, project_name)
                        time.sleep(1)
                    else:
                        with open(os.path.join(TMP_ARC_PATH, arcname), 'w') as f:
                            f.write('')

                    myzip.write(os.path.join(TMP_ARC_PATH, arcname), arcname=arcname)
                    os.remove(os.path.join(TMP_ARC_PATH, f'{word}_{word_need_count}.txt'))
                except:
                    pass

    return arc_path


def enter_markup_data(message, marker_id, project_name, sample_nums_info):
    out_str = ''
    cell = 0
    done_insert_value = 0

    if project_name == AVAIL_TXT_PROJECTS_NAMES[0]:
        sh = gc.open(HOMO_TABLE_NAME)
        worksheet = sh.get_worksheet(0)
        time.sleep(1)
        prev_marker_id = None
        for sample, sample_count in sample_nums_info.items():
            try:
                cell = worksheet.find(sample)
                true_marker_id = worksheet.cell(cell.row, HOMOGRAPHS_COORDINATES['marker_id']).value
                if true_marker_id == marker_id:

                    done_insert_value += sample_count
                    nums_done = int(worksheet.cell(cell.row, HOMOGRAPHS_COORDINATES['available_col']).value)
                    sum_words = nums_done + sample_count
                    words_done_count = int(
                        worksheet.cell(cell.row, HOMOGRAPHS_COORDINATES['word_done_count_col']).value)
                    if sum_words >= words_done_count:
                        mark_done = 'done'
                    else:
                        mark_done = 'hard'

                    out_str += move_file_to_yd(message, project_name, f'{sample}.txt', sample, marker_id,
                                               mark_done, prev_marker_id) + '\n'
                    worksheet.update_cell(cell.row, HOMOGRAPHS_COORDINATES['add_col'], sample_count)
                else:
                    out_str += f'Слово {sample} было выбрано другим разметчиком\n'
            except:
                out_str += f'Не удалось занести {sample} в таблицу\n'

    elif project_name == AVAIL_TXT_PROJECTS_NAMES[1]:
        sh = gc.open(HOMO_TABLE_NAME)
        worksheet = sh.get_worksheet(0)
        time.sleep(1)
        for sample, sample_count in sample_nums_info.items():
            try:
                for tmp_cell in worksheet.findall(sample):
                    if tmp_cell.col == HOMOGRAPHS_COORDINATES['main_words_col']:
                        cell = tmp_cell
                        break

                true_marker_id = worksheet.cell(cell.row, HOMOGRAPHS_COORDINATES['add_markup_marker_id']).value
                already_avail_num = int(worksheet.cell(cell.row, HOMOGRAPHS_COORDINATES['available_col']).value)
                words_done_count = int(worksheet.cell(cell.row, HOMOGRAPHS_COORDINATES['word_done_count_col']).value)
                sum_sample_count = already_avail_num + sample_count

                if true_marker_id == marker_id and sum_sample_count >= words_done_count:

                    if worksheet.cell(cell.row, HOMOGRAPHS_COORDINATES['add_col']).value:
                        add_sample_count = sample_count - int(
                            worksheet.cell(cell.row, HOMOGRAPHS_COORDINATES['add_col']).value)
                    else:
                        add_sample_count = sample_count

                    prev_marker_id = worksheet.cell(cell.row, HOMOGRAPHS_COORDINATES['marker_id']).value

                    done_insert_value += add_sample_count
                    out_str += move_file_to_yd(message, project_name, f'{sample}.txt', sample, marker_id, 'done',
                                               prev_marker_id) + '\n'
                    worksheet.update_cell(cell.row, HOMOGRAPHS_COORDINATES['add_markup_nums'], add_sample_count)

                elif sum_sample_count < words_done_count:
                    out_str += f'В омографе - {sample} - не хватает {words_done_count - sum_sample_count} примеров\n'

                else:
                    out_str += f'Слово {sample} было выбрано другим разметчиком\n'
            except:
                out_str += f'Не удалось занести {sample} в таблицу\n'

    elif project_name == AVAIL_TXT_PROJECTS_NAMES[2]:
        sh = gc.open(YOMO_TABLE_NAME)
        worksheet = sh.get_worksheet(0)
        time.sleep(1)
        for sample, sample_count in sample_nums_info.items():
            try:
                for tmp_cell in worksheet.findall(sample):
                    if tmp_cell.col == YOMOGRAPHS_COORDINATES['main_words_col']:
                        cell = tmp_cell
                        break

                true_marker_id = worksheet.cell(cell.row, YOMOGRAPHS_COORDINATES['add_markup_marker_id']).value
                already_avail_num = int(worksheet.cell(cell.row, YOMOGRAPHS_COORDINATES['available_col']).value)
                words_done_count = int(
                    worksheet.cell(cell.row, YOMOGRAPHS_COORDINATES['word_done_count_col']).value)
                sum_sample_count = already_avail_num + sample_count

                if true_marker_id == marker_id and sum_sample_count >= words_done_count:

                    if worksheet.cell(cell.row, YOMOGRAPHS_COORDINATES['sum_col']).value:
                        add_sample_count = sample_count - int(
                            worksheet.cell(cell.row, YOMOGRAPHS_COORDINATES['sum_col']).value)
                    else:
                        add_sample_count = sample_count

                    prev_marker_id = worksheet.cell(cell.row, YOMOGRAPHS_COORDINATES['marker_id']).value

                    done_insert_value += add_sample_count
                    out_str += move_file_to_yd(message, project_name, f'{sample}.txt', sample, marker_id, 'done',
                                               prev_marker_id) + '\n'
                    worksheet.update_cell(cell.row, YOMOGRAPHS_COORDINATES['add_markup_nums'], add_sample_count)

                elif sum_sample_count < words_done_count:
                    out_str += f'В омографе - {sample} - не хватает {words_done_count - sum_sample_count} примеров\n'

                else:
                    out_str += f'Слово {sample} было выбрано другим разметчиком\n'
            except:
                out_str += f'Не удалось занести {sample} в таблицу\n'

    # try:
    #     timetable = gc.open(MARKERS_NAMES_AND_TIMETABLES[marker_id][1])
    #     time_worksheet = timetable.worksheet(TIMETABLE_LIST)
    #     today = datetime.datetime.today().isoformat(sep=" ").split(' ')[0]
    #     date = f"{today.split('-')[2]}.{today.split('-')[1]}.{today.split('-')[0][2:]}"
    #     time_cell = time_worksheet.find(date)
    #     already_done_value = time_worksheet.cell(time_cell.row, TIMETABLE_DONE_COL).value
    #     if already_done_value:
    #         done_insert_value += int(already_done_value.split(',')[0])
    #     time_worksheet.update_cell(time_cell.row, TIMETABLE_DONE_COL, done_insert_value)
    # except:
    #     out_str += f'Не удалось занести данные в таблицу учета труда\n'

    return out_str


def insert_null_words(project_name, null_words, marker_id, insert_value):
    out_str = ''
    row = 0
    if project_name in AVAIL_TXT_PROJECTS_NAMES[:2]:
        sh = gc.open(HOMO_TABLE_NAME)
        worksheet = sh.get_worksheet(0)
        for word in null_words:
            try:
                for tmp_cell in worksheet.findall(word.strip()):
                    if tmp_cell.col == HOMOGRAPHS_COORDINATES['main_words_col']:
                        row = tmp_cell.row
                        break
                true_marker_id = worksheet.cell(row, HOMOGRAPHS_COORDINATES['marker_id']).value
                if true_marker_id == marker_id:
                    worksheet.update_cell(row, HOMOGRAPHS_COORDINATES['add_col'], insert_value)
                    out_str += f'Слово {word} добавлено в {project_name}\n'
                else:
                    out_str += f'Слово {word} было выбрано другим разметчиком\n'
            except:
                out_str += f'Не удалось занести {word} в таблицу\n'

    elif project_name == AVAIL_TXT_PROJECTS_NAMES[2]:
        sh = gc.open(YOMO_TABLE_NAME)
        worksheet = sh.get_worksheet(0)
        for word in null_words:
            try:
                for tmp_cell in worksheet.findall(word.strip()):
                    if tmp_cell.col == YOMOGRAPHS_COORDINATES['main_words_col']:
                        row = tmp_cell.row
                        break

                true_marker_id = worksheet.cell(row, YOMOGRAPHS_COORDINATES['marker_id']).value
                if true_marker_id == marker_id:
                    worksheet.update_cell(row, YOMOGRAPHS_COORDINATES['add_col'], insert_value)
                    out_str += f'Слово {word} добавлено в {project_name}\n'
                else:
                    out_str += f'Слово {word} было выбрано другим разметчиком\n'
            except:
                out_str += f'Не удалось занести {word} в таблицу\n'

    return out_str
