import sys
import os
cwd = os.getcwd()
sys.path.append(os.path.join(cwd, 'src', 'kafka_modules'))
sys.path.append(os.path.join(cwd, 'src', 'spark_modules'))

version = '1.0.0'
