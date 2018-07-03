from setuptools import setup

requirements = [
    'mysql-connector'
]

setup(
    name='pycyqle',
    version='0.0.1',
    description='python package to create model instances Database to model',
    author='Bruno Lange',
    author_email='blangeram@gmail.com',
    url='https://gitlab.com/brunolange/pycyql',
    install_requires=requirements,
    extras_require={
        'dev': [
            'pylint'
        ]
    }
)
