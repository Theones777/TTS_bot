import os
import re
import shutil
import zipfile
import tempfile

from utils.variables import TMP_ARC_PATH, TMP_DOWNLOAD_PATH


def check_file(path_file_markup, filename, flag):
    out_str = ''
    vowels = '[АОУЫЭЕЁИЮЯаоуыэеёиюя]'
    num_samples = 0
    without_duplicates = []
    try:
        with open(path_file_markup, 'r', encoding='utf-8') as file_markup:
            texts = file_markup.readlines()
    except:
        with open(path_file_markup, 'r', encoding='latin-1') as file_markup:
            texts = file_markup.readlines()

    idx_string_in_file = 1
    for string in texts:
        num_samples += 1
        errors = [str(idx_string_in_file)]
        if flag == 'text':
            if filename.split('.')[0] not in string.strip().lower():
                errors.append('нужный о/ёмограф не найден')
            if string not in without_duplicates:
                without_duplicates.append(string)
            else:
                dup_idx = without_duplicates.index(string)
                errors.append(f'Дубликат строки {dup_idx}')
        else:
            for i, proposal in enumerate(string.split('.')):
                if '?' in proposal and '*' not in proposal:
                    errors.append('нет спецсимвола в вопросительном предложении')
            for word in string.split(' '):
                if re.findall(vowels, word) and '+' not in word:
                    errors.append('нет ударения в слове')

        if len(string.strip()) > 0:
            if re.findall('\d', string.strip()):
                errors.append('ненормализованные цифры')
            if re.findall('[a-zA-Z]', string.strip()):
                errors.append('латиница')
                latin_letters = re.findall('[a-zA-Z]', string.strip())
                for latin_letter in latin_letters:
                    latin_index_start = str(string.strip().index(latin_letter) + 1)
                    latin_index_end = str(int(latin_index_start) + (len(latin_letter)))
                    metastr = f'{latin_letter}: ({latin_index_start}-{latin_index_end})'
                    errors.append(metastr)
            if re.findall('\. [а-я]', string.strip()):
                errors.append('после точки маленькая буква')
            if re.findall(': [А-Я]', string.strip()):
                errors.append('возможен обрыв контекста')
            if re.findall('[А-Я]{2,}', string.strip()):
                errors.append('возможна аббревиатура')
            if re.findall('[\?!,]\.', string.strip()):
                errors.append('двойные знаки препинания')
            if re.findall('[\?!,\.][А-Яа-я]', string.strip()):
                errors.append('отсутствует пробел после знака препинания')

            change_count = 0
            if re.findall('\w \.', string.strip()):
                change_count += 1
            if re.findall('\w \?', string.strip()):
                change_count += 1
            if re.findall('\w !', string.strip()):
                change_count += 1
            if re.findall('\w ,', string.strip()):
                change_count += 1
            if re.findall('\s{2,}', string.strip()):
                change_count += 1

            if change_count != 0:
                errors.append('Найдено лишних пробелов: ' + str(change_count))
            # if len(errors) > 2:
            out_str += ', '.join(errors) + '\n'

        idx_string_in_file += 1
    return out_str, num_samples


def check_input_files(files_path, marker_id, flag='txt'):
    if flag == 'txt':
        samples_nums_dict = {}
        reports_path = tempfile.mkdtemp()
        report_arc_path = os.path.join(TMP_ARC_PATH, f'report_{marker_id}.zip')
        with zipfile.ZipFile(report_arc_path, 'w') as myzip:
            for filename in os.listdir(files_path):
                file_report, samples_count = check_file(os.path.join(files_path, filename), filename, flag)
                samples_nums_dict[filename.split('.')[0]] = samples_count
                with open(os.path.join(reports_path, f'report_{filename}'), 'w', encoding='utf-8') as f:
                    f.write(file_report)
                myzip.write(os.path.join(reports_path, f'report_{filename}'), arcname=f'report_{filename}')
        shutil.rmtree(reports_path)
        return report_arc_path, samples_nums_dict
    else:
        unzip_path = os.path.join(TMP_DOWNLOAD_PATH, marker_id, 'unzip_files')
        os.makedirs(unzip_path, exist_ok=True)
        with zipfile.ZipFile(files_path) as zf:
            zf.extractall(unzip_path)

        txt_file = [fl for fl in os.listdir(unzip_path) if fl.endswith('.txt')][0]
        check_report, samples_nums = check_file(os.path.join(unzip_path, txt_file), txt_file, flag)
        if not check_report:
            check_report = 'Ошибок не найдено'
        return check_report, samples_nums


def convert_to_utf8(file_path):
    conv_filename = f'{file_path.split(os.sep)[-1]}_conv'
    conv_path = os.path.join(TMP_DOWNLOAD_PATH, 'convert')
    os.makedirs(conv_path, exist_ok=True)

    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            with open(os.path.join(conv_path, conv_filename), 'w', encoding='utf-8') as z:
                for string in f.readlines():
                    z.write(string.strip() + '\n')
    except:
        with open(file_path, 'r', encoding='cp1251') as f:
            with open(os.path.join(conv_path, conv_filename), 'w', encoding='utf-8') as z:
                for string in f.readlines():
                    z.write(string.strip() + '/n')

    os.remove(file_path)
    os.rename(os.path.join(conv_path, conv_filename), file_path)
