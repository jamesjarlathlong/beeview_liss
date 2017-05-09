import serial
import utime as time
import sys
uart = serial.Serial('/dev/ttymxc2', 9600)
def atap_command(t):
	toflush = uart.inWaiting()
	uart.read(toflush)
	uart.write('+++')
	time.sleep(t)
	waiting = uart.inWaiting()
	res = uart.read(waiting)
	if res ==b'OK\r':
		uart.write('ATAP1\r')
		time.sleep(t)
		waiting = uart.inWaiting()
		final_res = uart.read(waiting)
		if final_res ==b'OK\r':
			print('success')
			return
	else:
		print('oops, try again')
if __name__ == '__main__':
	t = sys.argv[1]
	atap_command(t)