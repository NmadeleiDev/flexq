import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='flexq',
    version='0.0.1',
    author='Gregory Potemkin',
    author_email='potemkin3940@gmail.com',
    description='Fleq is a python library for flexible management of task queues',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/NmadeleiDev/flexq',
    project_urls = {},
    packages=['flexq'],
    install_requires=['psycopg2-binary', 'uuid'],
)
