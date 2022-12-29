from aiogram.dispatcher.filters.state import StatesGroup, State


class Targets(StatesGroup):
    waiting_for_target = State()


class MarkUps(StatesGroup):
    waiting_for_project_name = State()
    waiting_for_project_info = State()


class UploadProjects(StatesGroup):
    waiting_for_project_name = State()
    waiting_for_null_words = State()
    waiting_for_file = State()
    waiting_for_report = State()
    waiting_for_confirm = State()


class CuratorsChecks(StatesGroup):
    waiting_for_project_name = State()
    waiting_for_curator_task = State()
    waiting_for_num_words = State()
    waiting_for_file = State()
    waiting_for_word = State()
    waiting_for_specific_word = State()
    waiting_for_indexes = State()


class SoundMan(StatesGroup):
    waiting_for_project_name = State()
    waiting_for_sound_man_task = State()
    waiting_for_audios_num = State()
    waiting_for_archive = State()
