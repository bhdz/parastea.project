from crawly.base import *
import os
import pathlib


urls = [
    'https://cdn2-thumbs.worldsex.com/albums/3/2130/217cb86920625e8d988e6b1e36b739b9a5f934e7_001_620x.jpg',
    'https://www.worldsex.com/porn-pics/busty-brunette-cougar-teases-with-her-juggs-2130/',
    'http://cdn2-thumbs.worldsex.com/albums/2130/3',
    'https://www.worldsex.com/porn-pics/busty-brunette-cougar-teases-with-her-juggs-2130/',
    'https://www.worldsex.com/porn-pics/busty-brunette-cougar-teases-with-her-juggs-2130/#pictureContainer',
    'https://cdn2-thumbs.worldsex.com/albums/3/2130/05861cd6fa2812d9bc75d2d9c7cfa90577da1021_001_620x.jpg',
    'https://cdn2-thumbs.worldsex.com/albums/3/2130/5d56920f42ee4b3f53d347266fac52814f0265bc_001_620x.jpg',
    'https://cdn2-thumbs.worldsex.com/albums/3/2130/217cb86920625e8d988e6b1e36b739b9a5f934e7_001_620x.jpg',
]

print(f"output_path: {output_path}")
print()


def print_identity(d: Downloaded):
    print(f"Downloaded.identity:")
    print(f" url: {d.identity.url}")
    print(f" dirname: {d.identity.dirname}")
    print(f" basename: {d.identity.basename}")
    print(f" filename: {d.identity.filename}")
    print(f" extension: {d.identity.extension}")
    print(f" path: {d.identity.path}")
    print(f" segment: {d.identity.segment}")
    print(f" server: {d.identity.server}")
    print(f" content_type: {d.identity.content_type}")
    print(f" parsed_url.scheme: {d.identity.parsed_url.scheme}")
    print(f" is_downloadable: {d.identity.is_downloadable}")
    print(f" is_text: {d.identity.is_text}")


for url in urls:
    d = Downloaded(url)

    dir_path = os.path.join(output_path,
                            d.identity.server, d.identity.segment)

    if not d.identity.extension:
        filename = 'index'
        extension = 'html'
        if d.identity.is_text:
            filename = 'index'
            extension = 'html'
        elif d.identity.is_downloadable:
            filename = 'index'
            extension = d.identity.content_type.split('/')[1]
        else:
            filename = 'index'
            extension = 'html'
        file_path = os.path.join(dir_path, f'{filename}.{extension}')
    else:
        file_path = os.path.join(dir_path, d.identity.basename)

    print(f" dir_path: {dir_path}")
    print(f' file_path: {file_path}')
    print()

    os.makedirs(dir_path, exist_ok=True)
    pathlib.Path(file_path).touch(exist_ok=True)

    if d.identity.is_downloadable:
        d.download_chunks(file_path)
    else:
        d.download(file_path)
