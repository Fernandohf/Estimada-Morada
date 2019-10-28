"""Example for text wrapping animation
"""
from __future__ import unicode_literals
import os
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from halo import Halo

spinner = Halo(text='dedededde', spinner='dots', animation='marquee')

try:
    spinner.start("")
    time.sleep(5)
    #spinner.succeed('End!')
    spinner.start("2")
    time.sleep(2)
    spinner.warn()
except (KeyboardInterrupt, SystemExit):
    spinner.stop()