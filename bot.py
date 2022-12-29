import asyncio

from handlers.common import register_handlers_common, set_commands
from handlers.curators import register_handlers_curators
from handlers.markups import register_handlers_markups
from handlers.targets import register_handlers_target
from handlers.uploads import register_handlers_uploads
from utils.bot_init import dp, bot


async def main():
    register_handlers_common(dp)
    register_handlers_target(dp)
    register_handlers_uploads(dp)
    register_handlers_markups(dp)
    register_handlers_curators(dp)

    await set_commands(bot)
    await dp.start_polling()

if __name__ == '__main__':
    asyncio.run(main())
