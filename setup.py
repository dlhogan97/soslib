from setuptools import find_packages, setup
setup(
    name='soslib',
    packages=find_packages(include=['soslib']),
    version='0.3.0',
    description='Library for use during and after the sublimation of snow campaign',
    author='Daniel Hogan',
    license='MIT',
    install_requires=['xarray','nctoolkit','numpy','pandas'],
    url='https://github.com/dlhogan97/soslib',
    setup_requires=['pytest-runner'],
    tests_require=['pytest==4.4.1'],
    test_suite='tests',
)
