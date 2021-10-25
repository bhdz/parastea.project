
import asyncio
from typing import Callable, Union, Any
from ioctools.base import CallableIO
from ioctools.base import CallableIO


class ApplyIO(CallableIO):
	def __init__(self, context):
		super().__init__()
		self.context = context

	def prepare(self, *args, **kwargs) -> bool:
		pass

	def forget(self, ret: Any) -> Any:
		pass


async def main():
	async def pause(amount: int) -> bool:
		print(f"pause: (amount = {amount}) -> begin:")

		await asyncio.sleep(amount)

		ret = True
		print(f"pause: -> done:")
	
	class Apply(ApplyIO):
		pass

	pause_once = ApplyIO()
