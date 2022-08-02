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
    packages=['flexq', 'flexq.workers', 'flexq.workers.threading_worker', 'flexq.jobstores', 'flexq.jobstores.postgres_jobstore', 'flexq.jobqueues', 'flexq.jobqueues.postgres_jobqueue'],
    install_requires=['psycopg2-binary', 'apscheduler'],
)
