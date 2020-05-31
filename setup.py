from distutils.core import setup
from Cython.Build import cythonize

# ext_options = {"compiler_directives": {"profile": True}, "annotate": True}
ext_options = {"language_level":2}
setup(
    ext_modules = cythonize("primes2.pyx", **ext_options)
)
