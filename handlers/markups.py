import os

from aiogram import Dispatcher, types
from aiogram.types import InputFile
from aiogram.dispatcher import FSMContext

from utils.tables.csv_data import get_audio_data
from utils.tables.google_sheets import get_text_markup_data
from utils.log import logging
from utils.states import MarkUps
from utils.variables import AVAIL_TXT_PROJECTS_NAMES, SUM_PROFILES, AVAIL_AUDIO_PROJECTS_NAMES


async def project_chosen(message: types.Message, state: FSMContext):
    logging(message)
    if message.text not in AVAIL_TXT_PROJECTS_NAMES + AVAIL_AUDIO_PROJECTS_NAMES:
        await message.answer("Пожалуйста, выберите проект, используя клавиатуру ниже.")
        return
    await state.update_data(chosen_project=message.text)
    await state.set_state(MarkUps.waiting_for_project_info.state)
    if message.text in AVAIL_TXT_PROJECTS_NAMES:
        await message.answer("Сколько слов хотите разметить?", reply_markup=types.ReplyKeyboardRemove())
    elif message.text in AVAIL_AUDIO_PROJECTS_NAMES:
        await message.answer("Загрузка аудио...", reply_markup=types.ReplyKeyboardRemove())
        marker_id = SUM_PROFILES[str(message.from_user.id)]
        arc_path = get_audio_data(message, marker_id)
        arc = InputFile(arc_path)
        await message.reply_document(arc, reply_markup=types.ReplyKeyboardRemove(), reply=False)
        os.remove(arc_path)
        await state.finish()


async def info_inserted(message: types.Message, state: FSMContext):
    logging(message)
    marker_id = SUM_PROFILES[str(message.from_user.id)]
    user_data = await state.get_data()
    project_name = user_data['chosen_project']
    arc_path = get_text_markup_data(message, marker_id, project_name)
    arc = InputFile(arc_path)
    await message.reply_document(arc)
    os.remove(arc_path)
    await state.finish()


def register_handlers_markups(dp: Dispatcher):
    dp.register_message_handler(project_chosen, state=MarkUps.waiting_for_project_name)
    dp.register_message_handler(info_inserted, state=MarkUps.waiting_for_project_info)
