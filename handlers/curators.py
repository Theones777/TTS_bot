import os

from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.types import InputFile
from aiogram.utils import markdown

from utils.bot_init import bot
from utils.checks.curators_check import enter_curator_data, get_tg_user_id
from utils.log import logging
from utils.states import CuratorsChecks
from utils.tables.csv_data import get_curator_words
from utils.variables import AVAIL_TXT_PROJECTS_NAMES, CURATORS_CHAT_ID, MARKERS_NAMES_AND_TIMETABLES, CURATOR_TASKS, \
    SUM_PROFILES, TMP_DOWNLOAD_PATH
from utils.yd_dir.yd_upload import upload_to_yd


async def project_chosen(message: types.Message, state: FSMContext):
    logging(message)
    if message.text not in AVAIL_TXT_PROJECTS_NAMES:
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
    elif message.text == CURATOR_TASKS[2]:
        await state.set_state(CuratorsChecks.waiting_for_file.state)
        await message.answer('Загрузите файлы (поставьте галочку "Группировать"',
                             reply_markup=types.ReplyKeyboardRemove())
    else:
        await state.set_state(CuratorsChecks.waiting_for_word.state)
        await message.answer("Введите проверяемое слово (как в таблице)", reply_markup=types.ReplyKeyboardRemove())


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
    await message.document.download(destination_file=download_file_path)
    out_str = upload_to_yd(project_name, download_file_path, file_name)[0]
    await message.answer(out_str, reply_markup=types.ReplyKeyboardRemove())
    await state.finish()


async def word_inserted(message: types.Message, state: FSMContext):
    logging(message)
    word = message.text
    await state.update_data(word=word)
    await state.set_state(CuratorsChecks.waiting_for_indexes.state)
    await message.answer("Введите результаты проверки в формате \nномер строки - категория ошибки shift+enter")


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
        await bot.send_message(CURATORS_CHAT_ID, f'{marker_link}\n*{word}*\n{curator_result}',
                               parse_mode="HTML", disable_web_page_preview=True)
    else:
        await message.answer(f'Ошибка! \n{curator_result}')


def register_handlers_curators(dp: Dispatcher):
    dp.register_message_handler(project_chosen, state=CuratorsChecks.waiting_for_project_name)
    dp.register_message_handler(curator_task_chosen, state=CuratorsChecks.waiting_for_curator_task)
    dp.register_message_handler(curator_num_words_inserted, state=CuratorsChecks.waiting_for_num_words)
    dp.register_message_handler(curator_file_upload, content_types=[types.ContentType.DOCUMENT],
                                state=CuratorsChecks.waiting_for_file)
    dp.register_message_handler(word_inserted, state=CuratorsChecks.waiting_for_word)
    dp.register_message_handler(indexes_inserted, state=CuratorsChecks.waiting_for_indexes)
