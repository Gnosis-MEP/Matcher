from setuptools import setup

setup(
    name='matcher',
    version='0.1',
    description='Simple matcher using redis graph and cypher to identify matching over inputed event windows',
    author='Felipe Arruda Pontes',
    author_email='felipe.arruda.pontes@insight-centre.org',
    packages=['matcher'],
    zip_safe=False
)
