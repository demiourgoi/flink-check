import socket
import time
import random
from concurrent.futures import ThreadPoolExecutor

def bucleDatos(sensor, iteraciones, c):
	i = 0
	while i < iteraciones:
		
		"""if(i < 20):
			pol = random.randint(20, 50)
		elif(20 <= i <= 50):
			pol = random.randint(0, 19)
		else:
			pol = random.randint(20, 50)"""

		pol = random.randint(0,30)
			
		c.send((str(pol) +"," + str(sensor) + '\n').encode())
		i = i+1
		time.sleep(0.5)
		print(pol)
	return 0

serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
serversocket.bind(('localhost', 9000))
serversocket.listen(5) # become a server socket, maximum 5 connections

c, a = serversocket.accept()

numSensores = 3
sensores = ThreadPoolExecutor(numSensores)
i = 0
lista = []
while i < numSensores:
	lista.append(sensores.submit(bucleDatos, i, 100, c))
	i = i+1

i = 0
while i < numSensores:
	while(lista[i].done()):
		time.sleep(0.5)
	i = i+1

time.sleep(10)
serversocket.close()
