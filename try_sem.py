import asyncio
from src.semaphore_manager import SemaphoreManager


async def job(manager, url):
    async with manager.acquire(url):
        await asyncio.sleep(0.05)


async def main():
    # 6 задач на один домен, лимит на домен 2
    m = SemaphoreManager(max_concurrent=10, max_per_domain=2)
    await asyncio.gather(*(job(m, f"https://a.com/{i}") for i in range(6)))
    print("один домен:", m.get_stats())

    # 3 домена по 4 задачи, общий лимит 3
    m = SemaphoreManager(max_concurrent=3, max_per_domain=2)
    await asyncio.gather(*(job(m, f"https://{d}/{i}")
                           for d in ("a.com", "b.com", "c.com") for i in range(4)))
    print("три домена:", m.get_stats())


asyncio.run(main())