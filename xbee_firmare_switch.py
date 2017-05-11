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
		print('switched mode')
		time.sleep(t)
		waiting = uart.inWaiting()
		final_res = uart.read(waiting)
		if final_res ==b'OK\r':
			uart.write('ATID7FFF\r')
			print('set channel id')
			time.sleep(t)
			waiting = uart.inWaiting()
			c_res = uart.read(waiting)
			if c_res ==b'OK\r':
				uart.write('ATWR\r')
				uart.write('ATFR\r')
				print('success')
				return
	else:
		print('oops, try again')
if __name__ == '__main__':
	t = float(sys.argv[1])
	atap_command(t)