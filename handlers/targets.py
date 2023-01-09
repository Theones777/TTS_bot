from aiogram import Dispatcher, types
from aiogram.dispatcher import FSMContext

from utils.log import logging
from utils.states import *
from utils.variables import AVAIL_TARGET_NAMES, AVAIL_TXT_PROJECTS_NAMES, CURATORS_PROFILES, SUM_PROFILES, \
    CURATORS_BUTTON, MARKERS_PROFILES, AVAIL_AUDIO_PROJECTS_NAMES, AVAIL_CURATORS_PROJECTS


async def bot_start(message: types.Message, state: FSMContext):
    await state.finish()
    if str(message.from_user.id) not in SUM_PROFILES.keys():
        await message.answer("У вас нет доступа к этому боту", reply_markup=types.ReplyKeyboardRemove())
    else:
        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
        if str(message.from_user.id) in CURATORS_PROFILES.keys():
            keyboard.add(CURATORS_BUTTON)
        if str(message.from_user.id) in MARKERS_PROFILES.keys():
            for name in AVAIL_TARGET_NAMES:
                keyboard.add(name)
        await state.set_state(Targets.waiting_for_target.state)
        await message.answer("Выберите задачу", reply_markup=keyboard)


async def target_chosen(message: types.Message, state: FSMContext):
    logging(message)
    target_name = message.text

    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    if target_name == AVAIL_TARGET_NAMES[0]:
        for name in AVAIL_TXT_PROJECTS_NAMES + AVAIL_AUDIO_PROJECTS_NAMES:
            keyboard.add(name)
        await message.answer("Выберите проект для разметки", reply_markup=keyboard)
        await state.set_state(MarkUps.waiting_for_project_name.state)

    elif target_name == AVAIL_TARGET_NAMES[1]:
        for name in AVAIL_TXT_PROJECTS_NAMES + AVAIL_AUDIO_PROJECTS_NAMES:
            keyboard.add(name)
        await message.answer("Выберите проект для загрузки файлов", reply_markup=keyboard)
        await state.set_state(UploadProjects.waiting_for_project_name.state)

    elif target_name == CURATORS_BUTTON:
        for name in AVAIL_CURATORS_PROJECTS:
            keyboard.add(name)
        await message.answer("Выберите проект для проверки", reply_markup=keyboard)
        await state.set_state(CuratorsChecks.waiting_for_project_name.state)

    # elif target_name == SOUND_MAN_BUTTON:
    #     for name in AVAIL_AUDIO_PROJECTS_NAMES:
    #         keyboard.add(name)
    #     await message.answer("Выберите проект с аудио", reply_markup=keyboard)
    #     await state.set_state(SoundMan.waiting_for_project_name.state)


def register_handlers_target(dp: Dispatcher):
    dp.register_message_handler(bot_start, commands="start", state="*")
    dp.register_message_handler(target_chosen, state=Targets.waiting_for_target)
