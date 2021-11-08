import setuptools

setuptools.setup(
    name='chatnoir-warc-indexer',
    version='0.0.1',
    install_requires=[
        'apache-beam',
        'boto3',
        'chardet',
        'click',
        'elasticsearch',
        'fastwarc',
        'html2text',
        'resiliparse',
    ],
    packages=setuptools.find_packages()
)
