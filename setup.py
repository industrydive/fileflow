from setuptools import setup

setup(
    name="fileflow",
    description="Airflow plugin to transfer arbitrary files between operators.",
    author='Industry Dive',
    author_email='fileflow@industrydive.com',
    url='https://github.com/industrydive/fileflow',
    license='Apache License 2.0',
    zip_safe=False,
    packages=['fileflow'],
    install_requires=[
        'airflow~=1.7.0',
        'pandas==0.17.0'
    ],
    test_suite='nose.collector',
    tests_require=['boto~=2.38.0', 'moto~=0.4.18', 'coverage~=4.2', 'nose~=1.3.7', 'mock~=1.0.1']
)