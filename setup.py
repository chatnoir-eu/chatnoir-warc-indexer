import setuptools

setuptools.setup(
    name='chatnoir-warc-indexer',
    version='0.0.1',
    url='https://resiliparse.chatnoir.eu',
    install_requires=[
        'apache-beam',
        'boto3',
        'click',
        'elasticsearch',
        'elasticsearch_dsl',
        'fastwarc',
        'python-dateutil',
        'redis',
        'resiliparse',
        'tqdm'
    ],
    packages=setuptools.find_packages(),
    entry_points={
        'console_scripts': ['chatnoir-index=warc_indexer.index:main']
    }
)
