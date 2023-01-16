import os
import shutil

from aiogram import types, Dispatcher, Bot
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Text
from aiogram.types import BotCommand

from utils.variables import TMP_DOWNLOAD_PATH, TMP_ARC_PATH, SUM_PROFILES


async def set_commands(bot: Bot):
    commands = [
        BotCommand(command="/start", description="Выбрать задачу"),
        BotCommand(command="/cancel", description="Вернуться в начало")
    ]
    await bot.set_my_commands(commands)


async def cmd_cancel(message: types.Message, state: FSMContext):
    await state.finish()
    user_id = SUM_PROFILES[str(message.from_user.id)]
    try:
        shutil.rmtree(os.path.join(TMP_DOWNLOAD_PATH, user_id))
    except:
        pass
    try:
        shutil.rmtree(TMP_ARC_PATH)
        os.makedirs(TMP_ARC_PATH, exist_ok=True)
    except:
        pass
    await message.answer("Сброс бота", reply_markup=types.ReplyKeyboardRemove())


def register_handlers_common(dp: Dispatcher):
    dp.register_message_handler(cmd_cancel, commands="cancel", state="*")
    dp.register_message_handler(cmd_cancel, Text(equals="отмена", ignore_case=True), state="*")
