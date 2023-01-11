import os
import shutil

from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.types import InputFile
from aiogram.utils import markdown

from utils.bot_init import bot
from utils.checks.curators_check import enter_curator_data, get_tg_user_id
from utils.log import logging
from utils.states import CuratorsChecks
from utils.tables.csv_data import get_curator_words, get_specific_word, enter_audio_data, get_long_audios_names, \
    insert_curator_id
from utils.variables import CURATORS_CHAT_ID, MARKERS_NAMES_AND_TIMETABLES, CURATOR_TASKS, \
    SUM_PROFILES, TMP_DOWNLOAD_PATH, AVAIL_CURATORS_PROJECTS, AVAIL_AUDIO_PROJECTS_NAMES, AVAIL_AUDIO_TEXT_TYPES, \
    YD_DICTORS_PROJECTS_PATH
from utils.yd_dir.yd_download import simple_download
from utils.yd_dir.yd_upload import upload_to_yd


async def project_chosen(message: types.Message, state: FSMContext):
    logging(message)
    if message.text not in AVAIL_CURATORS_PROJECTS:
        await message.answer("Пожалуйста, выберите проект, используя клавиатуру ниже.")
        return
    await state.update_data(chosen_project=message.text)
    await state.set_state(CuratorsChecks.waiting_for_curator_task.state)
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    for name in CURATOR_TASKS:
        keyboard.add(name)
    await message.answer("Выберите задачу", reply_markup=keyboard)


async def curator_task_chosen(message: types.Message, state: FSMContext):
    logging(message)
    await state.update_data(curator_task=message.text)
    if message.text == CURATOR_TASKS[0]:
        await state.set_state(CuratorsChecks.waiting_for_num_words.state)
        await message.answer("Сколько слов хотите проверить", reply_markup=types.ReplyKeyboardRemove())
    elif message.text == CURATOR_TASKS[1]:
        await state.set_state(CuratorsChecks.waiting_for_specific_word.state)
        await message.answer("Введите необходимое слово", reply_markup=types.ReplyKeyboardRemove())
    elif message.text == CURATOR_TASKS[2]:
        await state.set_state(CuratorsChecks.waiting_for_word.state)
        await message.answer("Введите проверяемое слово (как в таблице)", reply_markup=types.ReplyKeyboardRemove())
    else:
        await state.set_state(CuratorsChecks.waiting_for_file.state)
        await message.answer('Загрузите файлы (поставьте галочку "Группировать"',
                             reply_markup=types.ReplyKeyboardRemove())


async def curator_num_words_inserted(message: types.Message, state: FSMContext):
    curator_id = SUM_PROFILES[str(message.from_user.id)]
    user_data = await state.get_data()
    project_name = user_data['chosen_project']
    try:
        arc_path = get_curator_words(message, curator_id, project_name)
        arc = InputFile(arc_path)
        await message.reply_document(arc)
        os.remove(arc_path)
    except:
        await message.answer("Ошибка выдачи документов", reply_markup=types.ReplyKeyboardRemove())
    await state.finish()


async def curator_file_upload(message: types.Message, state: FSMContext):
    logging(message)
    curator_id = SUM_PROFILES[str(message.from_user.id)]
    file_name = message.document.file_name
    download_file_path = os.path.join(TMP_DOWNLOAD_PATH, curator_id, file_name)
    os.makedirs(os.path.join(TMP_DOWNLOAD_PATH, curator_id), exist_ok=True)
    user_data = await state.get_data()
    project_name = user_data['chosen_project']
    server_file = await bot.get_file(message.document.file_id)
    # cmd1 = f'docker cp 10fd0db6c46c:{server_file.file_path} {download_file_path}'
    # cmd2 = f'docker exec KononovTGServer rm -rf {server_file.file_path}'
    # os.system(cmd1)
    await message.document.download(destination_file=download_file_path)
    out_str = upload_to_yd(project_name, download_file_path, file_name)[0]
    await message.answer(out_str, reply_markup=types.ReplyKeyboardRemove())
    await state.finish()
    # os.system(cmd2)


async def word_inserted(message: types.Message, state: FSMContext):
    logging(message)
    word = message.text
    await state.update_data(word=word)
    await state.set_state(CuratorsChecks.waiting_for_indexes.state)
    await message.answer("Введите результаты проверки в формате \nномер строки - категория ошибки shift+enter")


async def specific_word_inserted(message: types.Message, state: FSMContext):
    logging(message)
    curator_id = SUM_PROFILES[str(message.from_user.id)]
    user_data = await state.get_data()
    project_name = user_data['chosen_project']
    try:
        file_path = get_specific_word(message, curator_id, project_name)
        file = InputFile(file_path)
        await message.reply_document(file, reply=False)
        os.remove(file_path)
    except Exception as e:
        logging(message, str(e))
        await message.answer("Ошибка выдачи документа", reply_markup=types.ReplyKeyboardRemove())
    await state.finish()


async def indexes_inserted(message: types.Message, state: FSMContext):
    logging(message)
    user_data = await state.get_data()
    project_name = user_data['chosen_project']
    word = user_data['word']
    marker_id, curator_result = enter_curator_data(message, message.text, word, project_name)
    await state.finish()
    if marker_id:
        tg_id = get_tg_user_id(marker_id)
        marker_link = markdown.hlink(MARKERS_NAMES_AND_TIMETABLES.get(marker_id)[0], f'tg://user?id={tg_id}')
        await bot.send_message(CURATORS_CHAT_ID, f'{marker_link}\n<b>{word}</b>\n{curator_result}',
                               parse_mode="HTML", disable_web_page_preview=True)
    else:
        await message.answer(f'Ошибка! \n{curator_result}')


async def dictor_chosen(message: types.Message, state: FSMContext):
    logging(message)
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    await state.update_data(dictor=message.text)
    for name in AVAIL_AUDIO_TEXT_TYPES:
        keyboard.add(name)
    await message.answer("Выберите тип текста", reply_markup=keyboard)
    await state.set_state(CuratorsChecks.waiting_for_text_type.state)


async def text_type_chosen(message: types.Message, state: FSMContext):
    logging(message)
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    await state.update_data(text_type=message.text)
    user_data = await state.get_data()
    dictor_name = user_data['dictor']
    long_audios_names = get_long_audios_names(dictor_name, message.text)
    for name in long_audios_names:
        keyboard.add(name)
    await message.answer("Выберите нужное аудио", reply_markup=keyboard)
    await state.set_state(CuratorsChecks.waiting_for_long_audio.state)


async def long_audio_project_chosen(message: types.Message, state: FSMContext):
    logging(message)
    user_data = await state.get_data()
    dictor_name = user_data['dictor']
    text_type = user_data['text_type']
    long_audio_name = message.text
    rpp_name = long_audio_name.split('/')[-1].replace('wav', 'rpp')
    curator_id = SUM_PROFILES[str(message.from_user.id)]
    filename = f'{dictor_name}_{text_type}_{rpp_name}'
    file_path = os.path.join(TMP_DOWNLOAD_PATH, curator_id, filename)
    os.makedirs(os.path.join(TMP_DOWNLOAD_PATH, curator_id), exist_ok=True)
    simple_download(f'{YD_DICTORS_PROJECTS_PATH}/{dictor_name}/{text_type}/{rpp_name}', file_path)
    file = InputFile(file_path)
    await message.reply_document(file, reply=False)
    os.remove(file_path)
    insert_curator_id(long_audio_name, curator_id)
    await state.finish()


async def audio_archive_upload(message: types.Message, state: FSMContext):
    logging(message)
    curator_id = SUM_PROFILES[str(message.from_user.id)]
    file_name = message.document.file_name
    download_file_path = os.path.join(TMP_DOWNLOAD_PATH, curator_id, file_name)
    os.makedirs(os.path.join(TMP_DOWNLOAD_PATH, curator_id), exist_ok=True)
    # server_file = await bot.get_file(message.document.file_id)
    # cmd1 = f'docker cp 10fd0db6c46c:{server_file.file_path} {download_file_path}'
    # cmd2 = f'docker exec KononovTGServer rm -rf {server_file.file_path}'
    # os.system(cmd1)
    await message.document.download(destination_file=download_file_path)

    await message.answer('Загрузка начата...', reply_markup=types.ReplyKeyboardRemove())
    out_str = enter_audio_data(message, curator_id, AVAIL_AUDIO_PROJECTS_NAMES[0],
                               flag='curator', file_path=download_file_path)
    await message.answer(out_str, reply_markup=types.ReplyKeyboardRemove())
    shutil.rmtree(os.path.join(TMP_DOWNLOAD_PATH, curator_id))
    # os.system(cmd2)


def register_handlers_curators(dp: Dispatcher):
    dp.register_message_handler(project_chosen, state=CuratorsChecks.waiting_for_project_name)
    dp.register_message_handler(dictor_chosen, state=CuratorsChecks.waiting_for_dictor_name)
    dp.register_message_handler(text_type_chosen, state=CuratorsChecks.waiting_for_text_type)
    dp.register_message_handler(long_audio_project_chosen, state=CuratorsChecks.waiting_for_long_audio)
    dp.register_message_handler(curator_task_chosen, state=CuratorsChecks.waiting_for_curator_task)
    dp.register_message_handler(curator_num_words_inserted, state=CuratorsChecks.waiting_for_num_words)
    dp.register_message_handler(curator_file_upload, content_types=[types.ContentType.DOCUMENT],
                                state=CuratorsChecks.waiting_for_file)
    dp.register_message_handler(audio_archive_upload, content_types=[types.ContentType.DOCUMENT],
                                state=CuratorsChecks.waiting_for_archive)
    dp.register_message_handler(word_inserted, state=CuratorsChecks.waiting_for_word)
    dp.register_message_handler(specific_word_inserted, state=CuratorsChecks.waiting_for_specific_word)
    dp.register_message_handler(indexes_inserted, state=CuratorsChecks.waiting_for_indexes)
