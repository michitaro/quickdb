import os
import subprocess

from .utils.chdir import chdir


def main():
    with chdir(os.path.join(os.path.dirname(__file__), '..')):
        for imagename in os.listdir('images'):
            with chdir(f'images/{imagename}'):
                subprocess.check_call(['docker', 'build', '-t', imagename, '.'])


if __name__ == '__main__':
    main()
