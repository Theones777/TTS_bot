import os
import shutil

from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.types import InputFile

from utils.bot_init import bot
from utils.checks.check import check_input_files
from utils.tables.csv_data import enter_audio_data
from utils.tables.google_sheets import enter_markup_data, insert_null_words
from utils.log import logging
from utils.states import UploadProjects
from utils.variables import AVAIL_TXT_PROJECTS_NAMES, TMP_DOWNLOAD_PATH, SUM_PROFILES, ADDITIONAL_UPLOAD_TXT_OPTIONS, \
    AVAIL_AUDIO_PROJECTS_NAMES


async def project_chosen(message: types.Message, state: FSMContext):
    logging(message)
    if message.text not in AVAIL_TXT_PROJECTS_NAMES + AVAIL_AUDIO_PROJECTS_NAMES:
        await message.answer("Пожалуйста, выберите проект, используя клавиатуру ниже.")
        return
    await state.update_data(chosen_project=message.text)
    await state.set_state(UploadProjects.waiting_for_file.state)
    if message.text == AVAIL_TXT_PROJECTS_NAMES[0]:
        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
        for name in ADDITIONAL_UPLOAD_TXT_OPTIONS:
            keyboard.add(name)
        await message.answer("Теперь загрузите файлы (поставьте галочку 'Группировать')"
                             "\n\nЕсли омографов не найдено либо слово не является омографом - нажмите на необходимую "
                             "кнопку",
                             reply_markup=keyboard)
    elif message.text == AVAIL_TXT_PROJECTS_NAMES[1:]:
        await message.answer("Теперь загрузите файлы (поставьте галочку 'Группировать')",
                             reply_markup=types.ReplyKeyboardRemove())
    elif message.text == AVAIL_AUDIO_PROJECTS_NAMES[0]:
        await message.answer("Теперь загрузите zip-архив с файлами (в имени архива не должно быть пробелов)",
                             reply_markup=types.ReplyKeyboardRemove())


async def null_words(message: types.Message, state: FSMContext):
    logging(message)
    await state.update_data(option_name=message.text)
    await message.answer('Теперь введите слова, через запятую (как в таблице)',
                         reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(UploadProjects.waiting_for_null_words.state)


async def null_words_inserted(message: types.Message, state: FSMContext):
    logging(message)

    user_data = await state.get_data()
    project_name = user_data['chosen_project']
    option_name = user_data['option_name']
    marker_id = SUM_PROFILES[str(message.from_user.id)]
    if option_name == ADDITIONAL_UPLOAD_TXT_OPTIONS[0]:
        insert_value = 0
    else:
        insert_value = -1
    out_str = insert_null_words(project_name, message.text.split(','), marker_id, insert_value)
    await state.finish()
    await message.answer(out_str)


async def upload_docs(message: types.Message, state: FSMContext):
    logging(message)
    marker_id = SUM_PROFILES[str(message.from_user.id)]
    raw_file_name = message.document.file_name

    user_data = await state.get_data()
    project_name = user_data['chosen_project']
    await message.answer('Идёт проверка файлов...', reply_markup=types.ReplyKeyboardRemove())
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    if project_name in AVAIL_TXT_PROJECTS_NAMES:
        file_name = f"{raw_file_name.split('_')[0]}.txt"
        keyboard.add('Получить информацию о возможных ошибках')

    elif project_name in AVAIL_AUDIO_PROJECTS_NAMES:
        file_name = raw_file_name
        for button in ['Подтвердить загрузку', 'Отменить загрузку']:
            keyboard.add(button)

    download_file_path = os.path.join(TMP_DOWNLOAD_PATH, marker_id, file_name)
    os.makedirs(os.path.join(TMP_DOWNLOAD_PATH, marker_id), exist_ok=True)
    # server_file = await bot.get_file(message.document.file_id)
    # print(server_file.file_path)
    # 'docker exec <container> rm -rf <YourFile>'
    await message.document.download(destination_file=download_file_path)

    if project_name in AVAIL_TXT_PROJECTS_NAMES:
        await state.update_data({'marker_id': marker_id})
        await state.set_state(UploadProjects.waiting_for_report.state)
        await message.answer(f"Файл {file_name} на проверке", reply_markup=keyboard)

    elif project_name in AVAIL_AUDIO_PROJECTS_NAMES:
        check_report, samples_nums = check_input_files(download_file_path, marker_id, flag='audio')
        await state.update_data({'samples_nums': samples_nums})
        await state.set_state(UploadProjects.waiting_for_confirm.state)
        await message.answer(check_report, reply_markup=keyboard)


async def check_files(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    marker_id = user_data['marker_id']
    files_path = os.path.join(TMP_DOWNLOAD_PATH, marker_id)

    report_arc_path, samples_nums_dict = check_input_files(files_path, marker_id)
    await state.update_data({'samples_nums_dict': samples_nums_dict})

    report_arc = InputFile(report_arc_path)
    await state.set_state(UploadProjects.waiting_for_confirm.state)
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    for button in ['Подтвердить загрузку', 'Отменить загрузку']:
        keyboard.add(button)
    await message.reply_document(report_arc, reply_markup=keyboard)
    os.remove(report_arc_path)


async def confirm_upload(message: types.Message, state: FSMContext):
    logging(message)
    user_data = await state.get_data()
    conclusion = message.text
    marker_id = SUM_PROFILES[str(message.from_user.id)]

    if conclusion == 'Подтвердить загрузку':
        await message.answer('Загрузка начата...', reply_markup=types.ReplyKeyboardRemove())
        project_name = user_data['chosen_project']
        samples_nums_dict = user_data.get('samples_nums_dict')
        samples_nums = user_data.get('samples_nums')
        sample_nums_info = samples_nums_dict if samples_nums_dict else samples_nums
        if project_name in AVAIL_TXT_PROJECTS_NAMES:
            out_str = enter_markup_data(message, marker_id, project_name, sample_nums_info)
        else:
            out_str = enter_audio_data(message, marker_id, project_name)
        await message.answer(out_str, reply_markup=types.ReplyKeyboardRemove())
    else:
        await message.answer('Загрузка отменена', reply_markup=types.ReplyKeyboardRemove())

    shutil.rmtree(os.path.join(TMP_DOWNLOAD_PATH, marker_id))

    await state.finish()


def register_handlers_uploads(dp: Dispatcher):
    dp.register_message_handler(project_chosen, state=UploadProjects.waiting_for_project_name)
    dp.register_message_handler(upload_docs, content_types=[types.ContentType.DOCUMENT],
                                state=UploadProjects.waiting_for_file)
    dp.register_message_handler(null_words, state=UploadProjects.waiting_for_file)
    dp.register_message_handler(null_words_inserted, state=UploadProjects.waiting_for_null_words)
    dp.register_message_handler(check_files, state=UploadProjects.waiting_for_report)
    dp.register_message_handler(confirm_upload, state=UploadProjects.waiting_for_confirm)
