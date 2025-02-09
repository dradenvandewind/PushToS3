gcc $(python3-config --cflags | sed "s/-g //g") -o app_asyncio app_asyncio.c $(python3-config --libs --embed)
