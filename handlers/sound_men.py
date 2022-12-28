import os

from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.types import InputFile

from utils.bot_init import bot
from utils.tables.csv_data import get_audio_data, enter_audio_data
from utils.log import logging
from utils.states import SoundMan
from utils.variables import AVAIL_AUDIO_PROJECTS_NAMES, SOUND_MAN_TASKS, SUM_PROFILES, TMP_DOWNLOAD_PATH


async def project_chosen(message: types.Message, state: FSMContext):
    logging(message)
    if message.text not in AVAIL_AUDIO_PROJECTS_NAMES:
        await message.answer("Пожалуйста, выберите проект, используя клавиатуру ниже.")
        return
    await state.update_data(chosen_project=message.text)
    await state.set_state(SoundMan.waiting_for_sound_man_task.state)
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    for name in SOUND_MAN_TASKS:
        keyboard.add(name)
    await message.answer("Выберите задачу", reply_markup=keyboard)


async def sound_man_task_chosen(message: types.Message, state: FSMContext):
    logging(message)
    await state.update_data(sound_man_task=message.text)
    if message.text == SOUND_MAN_TASKS[0]:
        await state.set_state(SoundMan.waiting_for_audios_num.state)
        await message.answer("Сколько аудио хотите нарезать?", reply_markup=types.ReplyKeyboardRemove())
    elif message.text == SOUND_MAN_TASKS[1]:
        await state.set_state(SoundMan.waiting_for_archive.state)
        await message.answer(
            'Загрузите zip-архив с нарезанными аудио (убедитесь, что в названиях файлов нет кириллицы)',
            reply_markup=types.ReplyKeyboardRemove())


async def audios_num_inserted(message: types.Message, state: FSMContext):
    sound_man_id = SUM_PROFILES[str(message.from_user.id)]
    user_data = await state.get_data()
    project_name = user_data['chosen_project']
    try:
        arc_path = get_audio_data(message, sound_man_id, project_name)
        arc = InputFile(arc_path)
        await message.reply_document(arc)
        os.remove(arc_path)
    except:
        await message.answer("Ошибка выдачи файлов", reply_markup=types.ReplyKeyboardRemove())
    await state.finish()


async def sound_man_archive_upload(message: types.Message, state: FSMContext):
    await message.answer('Загрузка...', reply_markup=types.ReplyKeyboardRemove())
    logging(message)
    sound_man_id = SUM_PROFILES[str(message.from_user.id)]
    file_name = message.document.file_name
    server_file = await bot.get_file(message.document.file_id)

    download_archive_path = os.path.join(TMP_DOWNLOAD_PATH, sound_man_id, file_name)
    os.makedirs(os.path.join(TMP_DOWNLOAD_PATH, sound_man_id), exist_ok=True)

    os.system(f'docker cp 10fd0db6c46c:{server_file.file_path} '
              f'{download_archive_path}')

    user_data = await state.get_data()
    project_name = user_data['chosen_project']
    out_str = enter_audio_data(message, sound_man_id, project_name, download_archive_path)
    await message.answer(out_str, reply_markup=types.ReplyKeyboardRemove())
    await state.finish()


def register_handlers_sound_man(dp: Dispatcher):
    dp.register_message_handler(project_chosen, state=SoundMan.waiting_for_project_name)
    dp.register_message_handler(sound_man_task_chosen, state=SoundMan.waiting_for_sound_man_task)
    dp.register_message_handler(audios_num_inserted, state=SoundMan.waiting_for_audios_num)
    dp.register_message_handler(sound_man_archive_upload, content_types=[types.ContentType.DOCUMENT],
                                state=SoundMan.waiting_for_archive)
