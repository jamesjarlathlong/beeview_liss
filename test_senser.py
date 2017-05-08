import asyncio
@asyncio.coroutine
def data_yielder():
    yield from asyncio.sleep(0.5)
    yield 0

def packager(senser):
	@asyncio.coroutine
	def sense(senss):
		result = yield from senss()
		return result
	return sense, senser

@asyncio.coroutine
def runner(package, fun):
    gen =  package(fun)
    for j in gen:
    	if isinstance(j, asyncio.Future):
    		yield j
    	else:
    		if j is not None:
    			print(j)
loop = asyncio.get_event_loop()
package, sense = packager(data_yielder)
loop.run_until_complete(runner(package, sense))



