from pyJoules.energy_meter import measure_energy
import time

@measure_energy
def foo():
    # pass
    time.sleep(10)
	# Instructions to be evaluated.

foo()
