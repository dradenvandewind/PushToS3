from setuptools import setup
from Cython.Build import cythonize

setup(
    ext_modules=cythonize("app_asyncio.py", compiler_directives={"language_level": "3str"})
)

