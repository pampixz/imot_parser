import asyncio
from twisted.internet import asyncioreactor

def install_reactor():
    """Безопасная установка реактора"""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        asyncioreactor.install(loop)
    except Exception:
        pass

if __name__ == "__main__":
    install_reactor()

    # ВАЖНО: импортировать bot только после установки реактора
    from bot import main
    main()

