from utils.tables.google_sheets import get_marker_id, insert_curator_info
from utils.log import logging
from utils.variables import SUM_PROFILES
from utils.yd_dir.yd_download import get_text


def enter_curator_data(message, curator_info, word, project_name):
    info_to_insert = str_to_out = ''
    mistakes_dict = {}
    info_to_out = {}
    try:
        for line in curator_info.split('\n'):
            mistakes_dict[line.split('-')[0].strip()] = line.split('-')[1].strip()
            info_to_out[line.split('-')[1].strip()] = []
    except:
        return 0, 'Неверный формат ввода ошибок'

    handle_mistakes_num = len(mistakes_dict)

    try:
        marker_id, add_markup_marker_id, status = get_marker_id(word, project_name)
    except:
        return 0, 'Не удалось получить данные из таблицы'

    logging(message, f'{marker_id}_{add_markup_marker_id}_{status}')

    try:
        text, upload_date = get_text(message, marker_id, add_markup_marker_id, status, word, project_name)
    except:
        return 0, 'Не удалось загрузить файл с Яндекс диска'

    for i in range(len(mistakes_dict)):
        key = list(mistakes_dict.keys())[i]
        value = mistakes_dict[key]
        if i == len(mistakes_dict) - 1:
            info_to_insert += f'{key} - {value} - {text[int(key)-1].rstrip()}'
        else:
            info_to_insert += f'{key} - {value} - {text[int(key)-1].rstrip()}\n'

        info_to_out[value].append(text[int(key)-1])

    if add_markup_marker_id:
        page_marker_id = add_markup_marker_id
    else:
        page_marker_id = marker_id

    try:
        insert_curator_info(info_to_insert, handle_mistakes_num, upload_date, page_marker_id)
    except:
        return 0, 'Не удалось занести данные в таблицу кураторов'

    # try:
    #     insert_to_timetable(handle_mistakes_num, upload_date, page_marker_id)
    # except:
    #     str_to_out += 'Не удалось занести данные в таблицу учета труда'

    for key, value in info_to_out.items():
        str_to_out += f'{key}:\n'
        str_to_out += ''.join(value)
    return page_marker_id, str_to_out


def get_tg_user_id(marker_id):
    for key, value in SUM_PROFILES.items():
        if value == marker_id:
            return int(key)
