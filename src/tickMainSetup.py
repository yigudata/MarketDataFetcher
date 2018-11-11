from cx_Freeze import setup, Executable

setup(name='tickDownloader',
      version='1.0.0',
      description='tick data downloading',
      executables=[Executable("tickMain.py")])
